package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/marlmelara/anchor/internal/agent"
	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/metrics"
	"github.com/marlmelara/anchor/internal/pricing"
	"github.com/marlmelara/anchor/internal/provider"
	"github.com/marlmelara/anchor/internal/store"
)

type outcome int

const (
	outcomeDone   outcome = iota // the run reached a terminal event
	outcomeParked                // requeued for later (retry backoff)
	outcomeLost                  // another worker owns this run
	outcomeError                 // something unrecoverable; the run was failed
)

// errLostOwnership unwinds the interpreter when an append loses the seq race.
var errLostOwnership = errors.New("lost ownership of run")

// interpret walks the graph and journals what happens.
//
// How resumption works, and why it needs no cursor in the database: the
// interpreter always walks the graph from the first node. Before doing any
// work it asks whether the step it is about to take already exists in the
// folded state. If it does, the recorded result is reused and the walk moves
// on; if it does not, the step executes. Step indexes are assigned sequentially
// as the walk proceeds, so a worker resuming a half-finished run replays the
// completed prefix in memory for free and picks up at exactly the first
// unfinished step.
// interpretFrom walks the graph, taking the claim time so the first step can
// report scheduling latency -- how long a run waited between being picked up
// and actually starting work.
func (w *Worker) interpretFrom(ctx context.Context, st *journal.RunState, graph *agent.Graph, claimedAt time.Time) outcome {
	ex := &execution{
		w:         w,
		st:        st,
		graph:     graph,
		claimedAt: claimedAt,
		outputs:   map[string]json.RawMessage{"input": nonEmptyJSON(st.Input)},
	}

	for _, node := range graph.Nodes {
		if err := ex.checkCancelled(ctx); err != nil {
			return ex.finish(ctx, err)
		}
		if err := ex.runNode(ctx, node); err != nil {
			return ex.finish(ctx, err)
		}
	}

	// The run's output is the last node's output.
	final := json.RawMessage(`{}`)
	if n := len(graph.Nodes); n > 0 {
		if out, ok := ex.outputs[graph.Nodes[n-1].ID]; ok {
			final = out
		}
	}
	if err := ex.append(ctx, store.PendingEvent{
		Type: journal.TypeRunCompleted,
		Payload: journal.RunCompletedPayload{
			Versioned: v(), Output: final,
		},
	}); err != nil {
		return ex.finish(ctx, err)
	}
	return outcomeDone
}

func v() journal.Versioned { return journal.Versioned{V: journal.CurrentPayloadVersion} }

func nonEmptyJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

// execution is one pass over the graph.
type execution struct {
	w     *Worker
	st    *journal.RunState
	graph *agent.Graph

	// outputs maps node id (and "input") to the value templates can reference.
	outputs map[string]json.RawMessage

	// cursor is the walk's position: the step index the next side-effecting
	// step will take. It counts steps the walk has reached, which on a resumed
	// run includes the ones replayed from the journal rather than executed.
	//
	// It must NOT be derived from len(st.Steps). That equals the walk position
	// only on a fresh run; on a resumed one the folded state already holds the
	// completed steps, so reading its length would start the walk past the end
	// and re-execute the whole graph at new indexes.
	cursor int

	// parkFor is set when a step needs to wait before its next attempt; the run
	// is requeued rather than the worker sleeping on it.
	parkFor time.Duration

	// claimedAt is when this worker took the run, for scheduling latency.
	claimedAt time.Time
	// firstStepObserved stops the latency histogram being fed once per step.
	firstStepObserved bool
}

// errBudget and errCancelled are terminal conditions that are not bugs.
var (
	// errTerminalJournaled means the run already has its terminal event in the
	// log, so the caller should unwind without writing another one.
	errTerminalJournaled = errors.New("run reached a terminal state")
	errRunCancelled      = errors.New("run cancelled")
	errParked            = errors.New("run parked for retry")
)

// checkCancelled stops at a step boundary if the run was cancelled or the
// worker lost its lease. Cancellation is cooperative: nothing is killed, the
// run simply stops advancing at a point where its state is consistent.
func (e *execution) checkCancelled(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if e.st.CancelRequested {
		return errRunCancelled
	}
	return nil
}

// append writes events and translates a lost seq race into errLostOwnership.
func (e *execution) append(ctx context.Context, events ...store.PendingEvent) error {
	err := e.w.store.Append(ctx, e.st, events...)
	if errors.Is(err, store.ErrSeqConflict) {
		return errLostOwnership
	}
	return err
}

// finish turns whatever ended the walk into the right terminal event.
func (e *execution) finish(ctx context.Context, cause error) outcome {
	switch {
	case errors.Is(cause, errLostOwnership):
		e.w.cfg.Logf("run %s: lost ownership mid-run", e.st.RunID)
		return outcomeLost

	case errors.Is(cause, errParked):
		availableAt := e.w.store.Now().Add(e.parkFor)
		if err := e.w.store.Requeue(ctx, e.st.RunID, e.w.cfg.WorkerID, availableAt, 0); err != nil {
			e.w.cfg.Logf("run %s: requeue: %v", e.st.RunID, err)
			return outcomeError
		}
		return outcomeParked

	case errors.Is(cause, errRunCancelled):
		// RunCancelled is already in the log -- the API wrote it. Nothing more
		// to append; the fold already reports the run as cancelled.
		return outcomeDone

	case errors.Is(cause, errTerminalJournaled):
		return outcomeDone

	case errors.Is(cause, context.Canceled), errors.Is(cause, context.DeadlineExceeded):
		// The worker is shutting down or lost its lease. Leave the run
		// unfinished: its lease expires and another worker resumes from the
		// journal. Writing a failure here would destroy recoverable work.
		e.w.cfg.Logf("run %s: stopped mid-run (%v)", e.st.RunID, cause)
		return outcomeLost

	default:
		e.w.failRun(ctx, e.st, cause.Error())
		return outcomeError
	}
}

// runNode dispatches one graph node.
func (e *execution) runNode(ctx context.Context, node agent.Node) error {
	switch node.Type {
	case agent.NodeModelCall:
		return e.runModelCall(ctx, node)
	case agent.NodeToolCall:
		return e.runToolCall(ctx, node)
	case agent.NodeAgentLoop:
		return e.runAgentLoop(ctx, node)
	default:
		// branch and map are validated but not yet interpreted. Failing loudly
		// beats silently skipping a node the author expected to run.
		return fmt.Errorf("node %q: type %q is not implemented yet", node.ID, node.Type)
	}
}

// ---------------------------------------------------------------------------
// model_call
// ---------------------------------------------------------------------------

func (e *execution) runModelCall(ctx context.Context, node agent.Node) error {
	prompt, err := agent.Render(node.Prompt, e.outputs)
	if err != nil {
		return fmt.Errorf("node %q: render prompt: %w", node.ID, err)
	}

	req := provider.Request{
		Model:     node.Model,
		System:    node.System,
		Messages:  []provider.Message{{Role: provider.RoleUser, Text: prompt}},
		MaxTokens: e.w.cfg.MaxTokensPerCall,
	}

	resp, err := e.modelStep(ctx, node.ID, node.Model, req)
	if err != nil {
		return err
	}
	e.outputs[node.ID] = modelOutput(resp)
	return nil
}

// modelOutput is what later templates see as {{nodeID.output}}.
func modelOutput(resp provider.Response) json.RawMessage {
	out := map[string]any{"output": resp.Text}
	if resp.ToolCall != nil {
		out["tool"] = resp.ToolCall.Name
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return json.RawMessage(`{"output":""}`)
	}
	return encoded
}

// modelStep performs one model call as a durable step: position the walk,
// journal, execute, journal the result, with retries.
func (e *execution) modelStep(ctx context.Context, nodeID, model string, req provider.Request) (provider.Response, error) {
	// Budgets are enforced in code before the call, never by asking the model
	// to behave -- a model that is misbehaving is exactly the one that will not
	// respect an instruction to stop.
	//
	// Both projections are deliberately pessimistic: the completion length is
	// unknown until after the money is spent, so the per-call ceiling is
	// assumed. A check that guessed low would let the call through and discover
	// the breach afterwards, which is what a budget exists to prevent.
	//
	// Checked before the cursor advances so a refused run does not consume a
	// step index it never used.
	if !e.stepAlreadyDone(e.cursor) {
		if over, ev := e.budgetWouldBreach(req); over {
			if err := e.append(ctx, ev...); err != nil {
				return provider.Response{}, err
			}
			return provider.Response{}, errTerminalJournaled
		}
	}

	// Shared protections, before anything is spent. Both park the run rather
	// than failing it: a provider that is rate limiting or briefly down is
	// expected to recover, and failing every queued run during an outage would
	// turn a transient problem into permanent loss.
	if !e.stepAlreadyDone(e.cursor) {
		decision, gerr := e.w.cfg.Guards.Check(ctx, e.st.TenantID)
		if gerr != nil {
			return provider.Response{}, fmt.Errorf("rate limit check: %w", gerr)
		}
		e.w.observe(func(m *metrics.Metrics) {
			m.ObserveBreakerState(e.w.cfg.Guards.Provider(), string(decision.BreakerState))
		})
		if !decision.Allowed {
			e.w.cfg.Logf("run %s: deferred by %s for %s", e.st.RunID, decision.Reason, decision.RetryAfter)
			e.w.observe(func(m *metrics.Metrics) {
				m.RateLimited.WithLabelValues(decision.Reason).Inc()
				if decision.Reason == "breaker_open" {
					m.BreakerRejections.WithLabelValues(e.w.cfg.Guards.Provider()).Inc()
				}
			})
			e.parkFor = decision.RetryAfter
			return provider.Response{}, errParked
		}
	}

	reqJSON, err := json.Marshal(req)
	if err != nil {
		return provider.Response{}, fmt.Errorf("marshal model request: %w", err)
	}

	// A model call is safe to repeat -- it costs money, not correctness -- so
	// it is treated as idempotent in the ambiguous window.
	decision, err := e.beginStep(ctx, journal.StepKindModel, model, nodeID, reqJSON, true)
	if err != nil {
		return provider.Response{}, err
	}
	if decision.recorded != nil {
		// Already completed, before a crash or on an earlier pass. Reuse it:
		// token accounting is already in the folded state, so re-adding it here
		// would double-count.
		return provider.Response{
			Raw:  decision.recorded.ModelResponse,
			Text: textFromRecorded(decision.recorded.ModelResponse),
		}, nil
	}
	stepIndex := decision.index
	req.StepIndex = stepIndex

	for {
		step, _ := e.st.Step(stepIndex)
		attempt := step.Attempt

		if err := e.append(ctx, store.PendingEvent{
			Type: journal.TypeModelCallStarted,
			Payload: journal.ModelCallStartedPayload{
				Versioned: v(), StepIndex: stepIndex, Attempt: attempt,
				Model: model, Request: reqJSON,
			},
		}); err != nil {
			return provider.Response{}, err
		}

		if !e.firstStepObserved {
			e.firstStepObserved = true
			e.w.observe(func(m *metrics.Metrics) {
				m.SchedulingLatency.Observe(e.w.store.Now().Sub(e.claimedAt).Seconds())
			})
		}

		callStarted := e.w.store.Now()
		callCtx, cancel := context.WithTimeout(ctx, e.w.cfg.ModelCallTimeout)
		resp, callErr := e.w.provider.Complete(callCtx, req)
		cancel()

		// A cancelled run is not the provider's fault, so it must not count
		// against the breaker -- a deploy that cancels in-flight work would
		// otherwise trip every breaker in the fleet.
		if ctx.Err() == nil {
			if state, rerr := e.w.cfg.Guards.Record(ctx, callErr == nil); rerr == nil {
				e.w.observe(func(m *metrics.Metrics) {
					m.ObserveBreakerState(e.w.cfg.Guards.Provider(), string(state))
				})
			}
		}
		e.w.observe(func(m *metrics.Metrics) {
			m.StepDuration.WithLabelValues("model").Observe(e.w.store.Now().Sub(callStarted).Seconds())
		})

		if callErr == nil {
			// The provider returns token counts, never cost. Money is computed
			// here from the price table; an unpriced model records
			// cost_unknown rather than a zero, because a silent zero is what
			// makes a budget look enforced when it is not.
			costMicros, costErr := pricing.Cost(model, resp.PromptTokens, resp.CompletionTokens)
			unknown := costErr != nil
			if unknown {
				e.w.cfg.Logf("run %s step %d: %v", e.st.RunID, stepIndex, costErr)
			}
			if err := e.append(ctx, store.PendingEvent{
				Type: journal.TypeModelCallCompleted,
				Payload: journal.ModelCallCompletedPayload{
					Versioned: v(), StepIndex: stepIndex, Attempt: attempt,
					Model: model, Response: nonEmptyJSON(resp.Raw),
					PromptTokens: resp.PromptTokens, CompletionTokens: resp.CompletionTokens,
					CostMicros: costMicros, CostUnknown: unknown,
				},
			}); err != nil {
				return provider.Response{}, err
			}
			e.w.observe(func(m *metrics.Metrics) {
				m.TokensUsed.WithLabelValues(model, "prompt").Add(float64(resp.PromptTokens))
				m.TokensUsed.WithLabelValues(model, "completion").Add(float64(resp.CompletionTokens))
				m.MicroDollarsSpent.WithLabelValues(model).Add(float64(costMicros))
			})
			return resp, nil
		}

		retry, err := e.handleStepFailure(ctx, stepIndex, attempt, callErr, true)
		if err != nil {
			return provider.Response{}, err
		}
		if !retry {
			return provider.Response{}, errParked
		}
	}
}

// budgetWouldBreach reports whether this call would exceed either budget, and
// the events to journal if so.
//
// A model with no price and a spending budget set is refused outright. The
// alternative -- proceeding with an uncountable cost -- leaves the operator
// believing a cap is in force when nothing is counting against it, which is
// worse than having declared no cap at all.
func (e *execution) budgetWouldBreach(req provider.Request) (bool, []store.PendingEvent) {
	fail := func(resource string, limit, used, projected int64, reason string) (bool, []store.PendingEvent) {
		return true, []store.PendingEvent{
			{Type: journal.TypeBudgetExceeded, Payload: journal.BudgetExceededPayload{
				Versioned: v(), Resource: resource,
				Limit: limit, Used: used, Projected: projected,
			}},
			{Type: journal.TypeRunFailed, Payload: journal.RunFailedPayload{
				Versioned: v(), Error: reason,
			}},
		}
	}

	if e.st.BudgetTokens > 0 {
		projected := e.st.TokensUsed + int64(e.w.cfg.MaxTokensPerCall)
		if projected > e.st.BudgetTokens {
			return fail("tokens", e.st.BudgetTokens, e.st.TokensUsed, projected, "token budget exceeded")
		}
	}

	if e.st.BudgetMicros > 0 {
		callCost, err := pricing.Projected(req.Model, int64(estimateTokens(req)), int64(e.w.cfg.MaxTokensPerCall))
		if err != nil {
			return fail("micros", e.st.BudgetMicros, e.st.MicrosUsed, 0,
				fmt.Sprintf("spending budget cannot be enforced: %v", err))
		}
		projected := e.st.MicrosUsed + callCost
		if projected > e.st.BudgetMicros {
			return fail("micros", e.st.BudgetMicros, e.st.MicrosUsed, projected,
				fmt.Sprintf("spending budget exceeded: cap %s, spent %s, this call would add %s",
					pricing.FormatMicros(e.st.BudgetMicros),
					pricing.FormatMicros(e.st.MicrosUsed),
					pricing.FormatMicros(callCost)))
		}
	}
	return false, nil
}

// estimateTokens approximates the prompt size before the provider counts it.
//
// Four bytes per token is a rough industry heuristic, not a real tokenizer.
// It only feeds the pre-call projection, which is already pessimistic on the
// output side; the authoritative count comes back with the response and is what
// the journal records.
func estimateTokens(req provider.Request) int {
	n := len(req.System)
	for _, m := range req.Messages {
		n += len(m.Text) + len(m.ToolResult)
	}
	for _, t := range req.Tools {
		n += len(t.Name) + len(t.Description) + len(t.InputSchema)
	}
	return n/4 + 1
}

// stepAlreadyDone reports whether the walk has already completed this index,
// so a resumed run does not re-evaluate a gate the original run passed.
func (e *execution) stepAlreadyDone(index int) bool {
	st, ok := e.st.Step(index)
	return ok && st.Status == journal.StepStatusCompleted
}

func textFromRecorded(raw json.RawMessage) string {
	var probe struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return ""
	}
	return probe.Text
}

// ---------------------------------------------------------------------------
// tool_call
// ---------------------------------------------------------------------------

func (e *execution) runToolCall(ctx context.Context, node agent.Node) error {
	input := nonEmptyJSON(node.Input)
	rendered, err := agent.Render(string(input), e.outputs)
	if err != nil {
		return fmt.Errorf("node %q: render tool input: %w", node.ID, err)
	}
	if !json.Valid([]byte(rendered)) {
		return fmt.Errorf("node %q: tool input is not valid JSON after rendering", node.ID)
	}

	out, err := e.toolStep(ctx, node.ID, node.Tool, json.RawMessage(rendered))
	if err != nil {
		return err
	}
	e.outputs[node.ID] = wrapToolOutput(out)
	return nil
}

func wrapToolOutput(out json.RawMessage) json.RawMessage {
	wrapped, err := json.Marshal(map[string]json.RawMessage{"output": nonEmptyJSON(out)})
	if err != nil {
		return json.RawMessage(`{"output":{}}`)
	}
	return wrapped
}

// toolStep runs one tool as a durable step.
func (e *execution) toolStep(ctx context.Context, nodeID, toolName string, input json.RawMessage) (json.RawMessage, error) {
	def, ok := e.w.tools.Get(toolName)
	if !ok {
		return nil, fmt.Errorf("node %q: unknown tool %q", nodeID, toolName)
	}

	decision, err := e.beginStep(ctx, journal.StepKindTool, toolName, nodeID, input, def.Idempotent)
	if err != nil {
		return nil, err
	}
	if decision.recorded != nil {
		return decision.recorded.ToolOutput, nil
	}
	stepIndex := decision.index

	for {
		step, _ := e.st.Step(stepIndex)
		attempt := step.Attempt

		if err := e.append(ctx, store.PendingEvent{
			Type: journal.TypeToolCallStarted,
			Payload: journal.ToolCallStartedPayload{
				Versioned: v(), StepIndex: stepIndex, Attempt: attempt,
				Tool: toolName, Input: input,
			},
		}); err != nil {
			return nil, err
		}

		toolStarted := e.w.store.Now()
		res, runErr := e.w.runner.Run(ctx, def, input)
		e.w.observe(func(m *metrics.Metrics) {
			m.StepDuration.WithLabelValues("tool").Observe(e.w.store.Now().Sub(toolStarted).Seconds())
		})
		if runErr == nil && res.ExitCode == 0 {
			if err := e.append(ctx, store.PendingEvent{
				Type: journal.TypeToolCallCompleted,
				Payload: journal.ToolCallCompletedPayload{
					Versioned: v(), StepIndex: stepIndex, Attempt: attempt,
					ExitCode: res.ExitCode, Output: nonEmptyJSON(res.Output), Stderr: res.Stderr,
				},
			}); err != nil {
				return nil, err
			}
			return nonEmptyJSON(res.Output), nil
		}

		if runErr == nil {
			runErr = fmt.Errorf("tool %q exited with code %d: %s", toolName, res.ExitCode, res.Stderr)
		}

		retry, err := e.handleStepFailure(ctx, stepIndex, attempt, runErr, def.Idempotent)
		if err != nil {
			return nil, err
		}
		if !retry {
			return nil, errParked
		}
	}
}

// ---------------------------------------------------------------------------
// agent_loop
// ---------------------------------------------------------------------------

func (e *execution) runAgentLoop(ctx context.Context, node agent.Node) error {
	specs := make([]provider.ToolSpec, 0, len(node.Tools))
	for _, name := range node.Tools {
		def, ok := e.w.tools.Get(name)
		if !ok {
			return fmt.Errorf("node %q: unknown tool %q", node.ID, name)
		}
		specs = append(specs, provider.ToolSpec{
			Name: def.Name, Description: def.Description, InputSchema: def.InputSchema,
		})
	}

	seed, err := agent.Render("{{input.output}}", e.outputs)
	if err != nil {
		// No templated seed: start the conversation from the raw run input.
		seed = string(nonEmptyJSON(e.st.Input))
	}
	messages := []provider.Message{{Role: provider.RoleUser, Text: seed}}

	for iteration := 0; iteration < node.MaxIterations; iteration++ {
		if err := e.checkCancelled(ctx); err != nil {
			return err
		}

		req := provider.Request{
			Model:     node.Model,
			System:    node.System,
			Messages:  messages,
			Tools:     specs,
			MaxTokens: e.w.cfg.MaxTokensPerCall,
		}
		resp, err := e.modelStep(ctx, node.ID, node.Model, req)
		if err != nil {
			return err
		}

		// No tool requested: this is the loop's final answer.
		if resp.ToolCall == nil {
			e.outputs[node.ID] = modelOutput(resp)
			return nil
		}

		out, err := e.toolStep(ctx, node.ID, resp.ToolCall.Name, nonEmptyJSON(resp.ToolCall.Input))
		if err != nil {
			return err
		}

		messages = append(messages,
			provider.Message{Role: provider.RoleAssistant, Text: resp.Text},
			provider.Message{
				Role: provider.RoleTool, ToolName: resp.ToolCall.Name, ToolResult: out,
			},
		)
	}

	return fmt.Errorf("node %q: agent_loop hit max_iterations (%d) without a final answer", node.ID, node.MaxIterations)
}

// ---------------------------------------------------------------------------
// shared step mechanics
// ---------------------------------------------------------------------------

// stepDecision is where the walk landed and what it found there.
type stepDecision struct {
	index int
	// recorded is non-nil when this step already completed, on an earlier pass
	// or before a crash. The caller reuses it instead of executing.
	recorded *journal.StepState
}

// beginStep advances the walk by one step and makes sure that step is
// scheduled, reserved, and safe to execute.
//
// Three cases, and the third is the interesting one:
//
//  1. The folded state has this step completed -> reuse it. This is how a
//     resumed run replays its finished prefix for free.
//  2. The folded state has it scheduled or failed -> an earlier attempt got
//     this far. Go straight to execution; re-journaling StepScheduled would be
//     rejected by the fold, which allows one per index.
//  3. The folded state has nothing here -> reserve the idempotency key and
//     journal StepScheduled. If the reservation reports the key ALREADY
//     EXISTS while the journal shows nothing, a previous worker reserved it
//     and died before journaling: the ambiguous window. The effect may or may
//     not have been applied, and only the tool's own declaration can say what
//     to do about that.
func (e *execution) beginStep(
	ctx context.Context,
	kind journal.StepKind,
	name, nodeID string,
	input json.RawMessage,
	idempotent bool,
) (stepDecision, error) {
	index := e.cursor
	e.cursor++

	if existing, ok := e.st.Step(index); ok {
		if existing.Status == journal.StepStatusCompleted {
			return stepDecision{index: index, recorded: existing}, nil
		}
		return stepDecision{index: index}, nil
	}

	tx, err := e.w.store.Pool().Begin(ctx)
	if err != nil {
		return stepDecision{}, fmt.Errorf("begin reservation: %w", err)
	}
	reservation, err := e.w.store.ReserveStep(ctx, tx, e.st.RunID, index, kind, name, input)
	if err != nil {
		_ = tx.Rollback(ctx)
		return stepDecision{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return stepDecision{}, fmt.Errorf("commit reservation: %w", err)
	}

	if reservation.AlreadyRan && !idempotent {
		idx := index
		if err := e.append(ctx, store.PendingEvent{
			Type: journal.TypeRunNeedsReview,
			Payload: journal.RunNeedsReviewPayload{
				Versioned: v(),
				Reason: fmt.Sprintf(
					"step %d (%s) was reserved by a worker that died before recording a result; "+
						"the tool is not idempotent, so whether its effect was applied cannot be determined",
					index, name),
				StepIndex: &idx,
			},
		}); err != nil {
			return stepDecision{}, err
		}
		return stepDecision{}, errTerminalJournaled
	}

	// Recorded so a replay reseeds identically. The generator is the
	// package-level one from math/rand/v2, which is safe for concurrent use;
	// what matters for determinism is that the value is journaled.
	seed := rand.Int64()
	if err := e.append(ctx, store.PendingEvent{
		Type: journal.TypeStepScheduled,
		Payload: journal.StepScheduledPayload{
			Versioned: v(), StepIndex: index, Kind: kind, Name: name,
			NodeID: nodeID, IdempotencyKey: reservation.Key, RandSeed: seed,
		},
	}); err != nil {
		return stepDecision{}, err
	}
	return stepDecision{index: index}, nil
}

// handleStepFailure journals the failure and decides what happens next.
//
// Returns retry=true to try again immediately, retry=false with errParked to
// requeue the run for later, or an error for a terminal outcome.
func (e *execution) handleStepFailure(
	ctx context.Context,
	stepIndex, attempt int,
	cause error,
	retryable bool,
) (bool, error) {
	// A cancelled context is not a step failure. The run is being stopped, and
	// journaling a failure would make recoverable work look broken.
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	isRetryable := retryable && provider.IsRetryable(cause)
	// A tool failure from an idempotent tool is safe to retry even when the
	// error carries no provider status.
	if retryable && !provider.IsRetryable(cause) {
		isRetryable = true
	}

	needsReview := !retryable
	exhausted := attempt+1 >= e.w.cfg.MaxAttempts

	if err := e.append(ctx, store.PendingEvent{
		Type: journal.TypeStepFailed,
		Payload: journal.StepFailedPayload{
			Versioned: v(), StepIndex: stepIndex, Attempt: attempt,
			Error: cause.Error(), Retryable: isRetryable, NeedsReview: needsReview,
		},
	}); err != nil {
		return false, err
	}
	if step, ok := e.st.Step(stepIndex); ok {
		e.w.observe(func(m *metrics.Metrics) {
			m.StepsFailed.WithLabelValues(string(step.Kind), fmt.Sprintf("%t", isRetryable)).Inc()
		})
	}

	switch {
	case needsReview:
		// Non-idempotent tool: the effect may or may not have landed and
		// nothing can tell us which. Halt for a human rather than guess.
		idx := stepIndex
		if err := e.append(ctx, store.PendingEvent{
			Type: journal.TypeRunNeedsReview,
			Payload: journal.RunNeedsReviewPayload{
				Versioned: v(),
				Reason: fmt.Sprintf(
					"step %d ran a non-idempotent tool that failed; the effect may or may not have been applied: %v",
					stepIndex, cause),
				StepIndex: &idx,
			},
		}); err != nil {
			return false, err
		}
		return false, errTerminalJournaled

	case !isRetryable, exhausted:
		if err := e.append(ctx, store.PendingEvent{
			Type: journal.TypeRunFailed,
			Payload: journal.RunFailedPayload{
				Versioned: v(),
				Error:     fmt.Sprintf("step %d failed after %d attempt(s): %v", stepIndex, attempt+1, cause),
				StepIndex: &stepIndex,
			},
		}); err != nil {
			return false, err
		}
		return false, errTerminalJournaled
	}

	delay := e.w.backoff(attempt)
	if err := e.append(ctx, store.PendingEvent{
		Type: journal.TypeRetryScheduled,
		Payload: journal.RetryScheduledPayload{
			Versioned: v(), StepIndex: stepIndex, Attempt: attempt + 1,
			DelayMS: delay.Milliseconds(),
		},
	}); err != nil {
		return false, err
	}

	// Short backoffs are waited out in place; a long one parks the run so the
	// goroutine is freed instead of holding a worker slot doing nothing.
	if delay <= time.Second {
		if !sleepCtx(ctx, delay) {
			return false, ctx.Err()
		}
		return true, nil
	}
	e.parkFor = delay
	return false, nil
}
