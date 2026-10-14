package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/marlmelara/anchor/internal/agent"
	"github.com/marlmelara/anchor/internal/journal"
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
func (w *Worker) interpret(ctx context.Context, st *journal.RunState, graph *agent.Graph) outcome {
	ex := &execution{
		w:       w,
		st:      st,
		graph:   graph,
		outputs: map[string]json.RawMessage{"input": nonEmptyJSON(st.Input)},
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

	// parkFor is set when a step needs to wait before its next attempt; the run
	// is requeued rather than the worker sleeping on it.
	parkFor time.Duration
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

// modelStep performs one model call as a durable step: reserve, journal,
// execute, journal the result, with retries.
func (e *execution) modelStep(ctx context.Context, nodeID, model string, req provider.Request) (provider.Response, error) {
	stepIndex := len(e.st.Steps)

	// Already done on an earlier attempt or before a crash: reuse the record
	// rather than paying for the call again.
	if done, resp, ok := e.recordedModelStep(stepIndex); ok {
		if done {
			return resp, nil
		}
	}

	req.StepIndex = stepIndex

	// Budgets are enforced in code before the call, never by asking the model
	// to behave. The projection is deliberately pessimistic: the per-call
	// ceiling, because the actual completion length is unknown until after the
	// money is spent.
	projected := e.st.TokensUsed + int64(e.w.cfg.MaxTokensPerCall)
	if e.st.BudgetTokens > 0 && projected > e.st.BudgetTokens {
		if err := e.append(ctx,
			store.PendingEvent{Type: journal.TypeBudgetExceeded, Payload: journal.BudgetExceededPayload{
				Versioned: v(), Resource: "tokens",
				Limit: e.st.BudgetTokens, Used: e.st.TokensUsed, Projected: projected,
			}},
			store.PendingEvent{Type: journal.TypeRunFailed, Payload: journal.RunFailedPayload{
				Versioned: v(), Error: "token budget exceeded",
			}},
		); err != nil {
			return provider.Response{}, err
		}
		return provider.Response{}, errTerminalJournaled
	}

	reqJSON, err := json.Marshal(req)
	if err != nil {
		return provider.Response{}, fmt.Errorf("marshal model request: %w", err)
	}

	if err := e.scheduleStep(ctx, stepIndex, journal.StepKindModel, model, nodeID, reqJSON); err != nil {
		return provider.Response{}, err
	}

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

		callCtx, cancel := context.WithTimeout(ctx, e.w.cfg.ModelCallTimeout)
		resp, callErr := e.w.provider.Complete(callCtx, req)
		cancel()

		if callErr == nil {
			if err := e.append(ctx, store.PendingEvent{
				Type: journal.TypeModelCallCompleted,
				Payload: journal.ModelCallCompletedPayload{
					Versioned: v(), StepIndex: stepIndex, Attempt: attempt,
					Model: model, Response: nonEmptyJSON(resp.Raw),
					PromptTokens: resp.PromptTokens, CompletionTokens: resp.CompletionTokens,
					Cents: resp.Cents,
				},
			}); err != nil {
				return provider.Response{}, err
			}
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

// recordedModelStep reports whether this step already completed, and with what.
func (e *execution) recordedModelStep(stepIndex int) (done bool, resp provider.Response, ok bool) {
	step, exists := e.st.Step(stepIndex)
	if !exists {
		return false, provider.Response{}, false
	}
	if step.Status != journal.StepStatusCompleted {
		return false, provider.Response{}, true
	}
	// Reconstruct just enough for the walk to continue. Token accounting is
	// already in the folded state; re-adding it here would double-count.
	return true, provider.Response{
		Raw:   step.ModelResponse,
		Text:  textFromRecorded(step.ModelResponse),
		Cents: step.Cents,
	}, true
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
	stepIndex := len(e.st.Steps)

	if step, exists := e.st.Step(stepIndex); exists && step.Status == journal.StepStatusCompleted {
		return step.ToolOutput, nil
	}

	def, ok := e.w.tools.Get(toolName)
	if !ok {
		return nil, fmt.Errorf("node %q: unknown tool %q", nodeID, toolName)
	}

	if err := e.scheduleStep(ctx, stepIndex, journal.StepKindTool, toolName, nodeID, input); err != nil {
		return nil, err
	}

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

		res, runErr := e.w.runner.Run(ctx, def, input)
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

		// A non-idempotent tool that failed may still have had its effect.
		// Anchor refuses to guess: the run halts for a human.
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

// scheduleStep reserves the idempotency key and journals StepScheduled.
//
// Journal-before-effect: StepScheduled is written before anything happens, so a
// crash leaves evidence that the step was about to run. The reservation and the
// event are not in one transaction by accident -- the reservation has to be
// visible to every worker immediately, including one that takes over after this
// one dies.
func (e *execution) scheduleStep(
	ctx context.Context,
	stepIndex int,
	kind journal.StepKind,
	name, nodeID string,
	input json.RawMessage,
) error {
	tx, err := e.w.store.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin reservation: %w", err)
	}
	reservation, err := e.w.store.ReserveStep(ctx, tx, e.st.RunID, stepIndex, kind, name, input)
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reservation: %w", err)
	}

	seed := e.w.rng.Int63()
	return e.append(ctx, store.PendingEvent{
		Type: journal.TypeStepScheduled,
		Payload: journal.StepScheduledPayload{
			Versioned: v(), StepIndex: stepIndex, Kind: kind, Name: name,
			NodeID: nodeID, IdempotencyKey: reservation.Key, RandSeed: seed,
		},
	})
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
