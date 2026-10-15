// Package replay re-executes a recorded run against its journal instead of the
// world.
//
// A replay is a REAL run: it is claimed from the queue, walks the same graph,
// journals its own events, and is subject to the same fold. The only difference
// is what it is holding. Where a live run holds an Anthropic client, a Docker
// sandbox and the wall clock, a replay holds three objects that answer from the
// original run's journal -- and the worker cannot tell the difference, because
// all three satisfy the same interfaces.
//
// That is the whole design. Replay is not a separate code path; it is the same
// code path with different dependencies. Anything that only happens on the live
// path would be untested by replay, and anything replay does differently would
// be a divergence the real run never had.
package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/provider"
	"github.com/marlmelara/anchor/internal/tool"
)

// Recording is everything the original run observed, keyed by step index.
//
// Built by folding the original journal -- the same fold the worker uses, not a
// second parser. If the fold can reconstruct state from events, it can also
// reconstruct what each step saw.
type Recording struct {
	RunID     string
	AgentName string
	AgentVer  int
	Input     json.RawMessage
	Steps     map[int]RecordedStep
	// Times are the created_at of every original event, in order. The replay
	// clock walks this list, so code asking for the time gets the time the
	// original run saw at that point.
	Times []time.Time
	// Events is the original event sequence, for the divergence comparison.
	Events []Signature
	// TerminalOutput is what the original run finally produced.
	TerminalOutput json.RawMessage
	Status         journal.RunStatus
}

// RecordedStep is one step's captured non-determinism.
//
// Outcomes are keyed by ATTEMPT, not collapsed to the final result. A step that
// failed once and then succeeded must replay the failure too: returning the
// success immediately would make the replay skip StepFailed and RetryScheduled,
// producing a shorter event sequence and diverging for a reason that is not a
// real bug. Every run that ever retried would score as a failure.
type RecordedStep struct {
	Index    int
	Kind     journal.StepKind
	Name     string
	NodeID   string
	RandSeed int64

	Attempts map[int]Outcome
}

// Outcome is what one attempt of a step produced.
type Outcome struct {
	// Failed is set when the attempt ended in StepFailed. The replay returns an
	// error so the interpreter takes the same retry path the original took.
	Failed    bool
	Error     string
	Retryable bool

	// ModelResponse is the provider's full body, returned verbatim.
	ModelResponse json.RawMessage
	// ToolOutput and ToolExitCode are what the container produced.
	ToolOutput   json.RawMessage
	ToolExitCode int

	PromptTokens     int64
	CompletionTokens int64
}

// Signature is the comparable shape of one event.
//
// Deliberately NOT the whole event. Timestamps differ between a run and its
// replay by definition -- the replay happens later -- and the event id is a
// sequence number from the database. Comparing those would make every replay
// diverge at event one. What must match is the SHAPE: which events happened, in
// what order, about which step.
type Signature struct {
	Seq  int               `json:"seq"`
	Type journal.EventType `json:"type"`
	// StepIndex and Attempt are -1 when the event has no step, rather than
	// pointers. A struct with pointer fields looks comparable with == but
	// compares the POINTERS, which silently reports two identical signatures as
	// different -- a trap that cost a test here. Plain ints make == correct.
	StepIndex int `json:"step_index"`
	Attempt   int `json:"attempt"`
}

// NoStep marks a signature field the event does not carry.
const NoStep = -1

func (s Signature) String() string {
	out := fmt.Sprintf("seq %d %s", s.Seq, s.Type)
	if s.StepIndex != NoStep {
		out += fmt.Sprintf(" step=%d", s.StepIndex)
	}
	if s.Attempt != NoStep {
		out += fmt.Sprintf(" attempt=%d", s.Attempt)
	}
	return out
}

// Build turns an original run's events into a Recording.
//
// Walks the raw EVENTS for per-attempt outcomes rather than reading the folded
// state, because the fold deliberately collapses attempts -- a retried step
// shows only its latest status. Replay needs the history the fold throws away.
// The fold is still used for identity and terminal state, where it is
// authoritative.
func Build(events []journal.Event) (*Recording, error) {
	st, err := journal.Fold(events)
	if err != nil {
		return nil, fmt.Errorf("fold original run: %w", err)
	}

	rec := &Recording{
		RunID:          st.RunID.String(),
		AgentName:      st.AgentName,
		AgentVer:       st.AgentVersion,
		Input:          st.Input,
		Steps:          make(map[int]RecordedStep, len(st.Steps)),
		Times:          make([]time.Time, 0, len(events)),
		Events:         make([]Signature, 0, len(events)),
		TerminalOutput: st.TerminalOutput,
		Status:         st.Status,
	}

	ensure := func(idx int) RecordedStep {
		step, ok := rec.Steps[idx]
		if !ok {
			step = RecordedStep{Index: idx, Attempts: map[int]Outcome{}}
		}
		return step
	}

	for _, e := range events {
		rec.Times = append(rec.Times, e.CreatedAt)
		sig, sigErr := SignatureOf(e)
		if sigErr != nil {
			return nil, sigErr
		}
		rec.Events = append(rec.Events, sig)

		switch e.Type {
		case journal.TypeStepScheduled:
			var p journal.StepScheduledPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				return nil, fmt.Errorf("seq %d: %w", e.Seq, err)
			}
			step := ensure(p.StepIndex)
			step.Kind, step.Name, step.NodeID, step.RandSeed = p.Kind, p.Name, p.NodeID, p.RandSeed
			rec.Steps[p.StepIndex] = step

		case journal.TypeModelCallCompleted:
			var p journal.ModelCallCompletedPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				return nil, fmt.Errorf("seq %d: %w", e.Seq, err)
			}
			step := ensure(p.StepIndex)
			step.Attempts[p.Attempt] = Outcome{
				ModelResponse:    p.Response,
				PromptTokens:     p.PromptTokens,
				CompletionTokens: p.CompletionTokens,
			}
			rec.Steps[p.StepIndex] = step

		case journal.TypeToolCallCompleted:
			var p journal.ToolCallCompletedPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				return nil, fmt.Errorf("seq %d: %w", e.Seq, err)
			}
			step := ensure(p.StepIndex)
			step.Attempts[p.Attempt] = Outcome{
				ToolOutput:   p.Output,
				ToolExitCode: p.ExitCode,
			}
			rec.Steps[p.StepIndex] = step

		case journal.TypeStepFailed:
			var p journal.StepFailedPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				return nil, fmt.Errorf("seq %d: %w", e.Seq, err)
			}
			step := ensure(p.StepIndex)
			step.Attempts[p.Attempt] = Outcome{
				Failed: true, Error: p.Error, Retryable: p.Retryable,
			}
			rec.Steps[p.StepIndex] = step
		}
	}
	return rec, nil
}

// SignatureOf extracts the comparable shape of one event.
func SignatureOf(e journal.Event) (Signature, error) {
	sig := Signature{Seq: e.Seq, Type: e.Type, StepIndex: NoStep, Attempt: NoStep}
	var probe struct {
		StepIndex *int `json:"step_index"`
		Attempt   *int `json:"attempt"`
	}
	if err := json.Unmarshal(e.Payload, &probe); err != nil {
		return sig, fmt.Errorf("read signature of %s at seq %d: %w", e.Type, e.Seq, err)
	}
	if probe.StepIndex != nil {
		sig.StepIndex = *probe.StepIndex
	}
	if probe.Attempt != nil {
		sig.Attempt = *probe.Attempt
	}
	return sig, nil
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

// Provider answers model calls from the recording instead of the network.
//
// It satisfies provider.Provider, so the worker holding it is byte-identical to
// the worker holding an Anthropic client.
type Provider struct {
	rec *Recording
	mu  sync.Mutex
	// divergence is the FIRST mismatch seen. Only the first is kept: once a
	// replay has diverged, everything after it is downstream of that one
	// difference and reporting it all would bury the cause.
	divergence *Divergence
}

// NewProvider builds a replay provider over a recording.
func NewProvider(rec *Recording) *Provider { return &Provider{rec: rec} }

func (p *Provider) Name() string { return "replay" }

// Divergence returns the first mismatch, or nil.
func (p *Provider) Divergence() *Divergence {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.divergence
}

func (p *Provider) note(d *Divergence) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.divergence == nil {
		p.divergence = d
	}
}

// Complete returns what the original run received at this step and attempt.
func (p *Provider) Complete(ctx context.Context, req provider.Request) (provider.Response, error) {
	step, ok := p.rec.Steps[req.StepIndex]
	if !ok {
		// The replay reached a model call the original never made. The graph
		// took a different path, which is the most serious kind of divergence.
		d := &Divergence{
			StepIndex: req.StepIndex,
			Kind:      DivergenceUnexpectedCall,
			Detail:    fmt.Sprintf("replay made a model call at step %d; the original run had no step there", req.StepIndex),
		}
		p.note(d)
		return provider.Response{}, d
	}
	if step.Kind != journal.StepKindModel {
		d := &Divergence{
			StepIndex: req.StepIndex,
			Kind:      DivergenceWrongStepKind,
			Detail:    fmt.Sprintf("replay made a model call at step %d, but the original ran a %s there", req.StepIndex, step.Kind),
		}
		p.note(d)
		return provider.Response{}, d
	}

	outcome, ok := step.Attempts[req.Attempt]
	if !ok {
		d := &Divergence{
			StepIndex: req.StepIndex,
			Kind:      DivergenceUnexpectedCall,
			Detail: fmt.Sprintf("replay made attempt %d of step %d; the original made %d attempt(s)",
				req.Attempt, req.StepIndex, len(step.Attempts)),
		}
		p.note(d)
		return provider.Response{}, d
	}

	if outcome.Failed {
		// Replay the FAILURE, so the interpreter takes the same retry path the
		// original took and produces the same events.
		err := fmt.Errorf("replayed failure: %s", outcome.Error)
		if outcome.Retryable {
			return provider.Response{}, &provider.RetryableError{Err: err}
		}
		return provider.Response{}, err
	}

	return provider.Response{
		Raw:              outcome.ModelResponse,
		Text:             textOf(outcome.ModelResponse),
		ToolCall:         toolCallOf(outcome.ModelResponse),
		PromptTokens:     outcome.PromptTokens,
		CompletionTokens: outcome.CompletionTokens,
	}, nil
}

// ---------------------------------------------------------------------------
// Tool runner
// ---------------------------------------------------------------------------

// Runner answers tool calls from the recording instead of a container.
type Runner struct {
	rec      *Recording
	provider *Provider // shared divergence sink
}

// NewRunner builds a replay tool runner sharing the provider's divergence sink,
// so the FIRST divergence across both is the one reported.
func NewRunner(rec *Recording, p *Provider) *Runner {
	return &Runner{rec: rec, provider: p}
}

// Run returns the recorded output for this tool step and attempt.
//
// The position comes from the context rather than from counting calls. Counting
// would assume the replay visits tool steps in the same order -- which is the
// very thing under test, so assuming it would make the check unable to detect
// the failure it exists to find.
func (r *Runner) Run(ctx context.Context, def tool.Definition, input json.RawMessage) (tool.Result, error) {
	pos, ok := tool.PositionFrom(ctx)
	if !ok {
		d := &Divergence{
			Kind:   DivergenceUnexpectedCall,
			Detail: fmt.Sprintf("tool %q was called without a step position; the replay runner cannot locate the recording", def.Name),
		}
		r.provider.note(d)
		return tool.Result{}, d
	}

	step, ok := r.rec.Steps[pos.StepIndex]
	if !ok {
		d := &Divergence{
			StepIndex: pos.StepIndex,
			Kind:      DivergenceUnexpectedCall,
			Detail:    fmt.Sprintf("replay ran tool %q at step %d; the original had no step there", def.Name, pos.StepIndex),
		}
		r.provider.note(d)
		return tool.Result{}, d
	}
	if step.Kind != journal.StepKindTool {
		d := &Divergence{
			StepIndex: pos.StepIndex,
			Kind:      DivergenceWrongStepKind,
			Detail:    fmt.Sprintf("replay ran a tool at step %d, but the original ran a %s there", pos.StepIndex, step.Kind),
		}
		r.provider.note(d)
		return tool.Result{}, d
	}
	if step.Name != def.Name {
		d := &Divergence{
			StepIndex: pos.StepIndex,
			Kind:      DivergenceWrongTool,
			Detail: fmt.Sprintf("replay called tool %q at step %d; the original called %q",
				def.Name, pos.StepIndex, step.Name),
		}
		r.provider.note(d)
		return tool.Result{}, d
	}

	outcome, ok := step.Attempts[pos.Attempt]
	if !ok {
		d := &Divergence{
			StepIndex: pos.StepIndex,
			Kind:      DivergenceUnexpectedCall,
			Detail: fmt.Sprintf("replay made attempt %d of tool step %d; the original made %d",
				pos.Attempt, pos.StepIndex, len(step.Attempts)),
		}
		r.provider.note(d)
		return tool.Result{}, d
	}
	if outcome.Failed {
		return tool.Result{}, fmt.Errorf("replayed failure: %s", outcome.Error)
	}
	return tool.Result{Output: outcome.ToolOutput, ExitCode: outcome.ToolExitCode}, nil
}

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

// Clock returns the times the original run saw, in order.
//
// Any code reading the real clock would take a different path on replay -- "skip
// this if it is after 5pm" is the classic. Routing every read through a Clock
// makes time an input that can be recorded, which is the only way it becomes
// replayable.
type Clock struct {
	mu    sync.Mutex
	times []time.Time
	i     int
	// last is returned once the recorded times run out, so a replay that makes
	// one extra call gets a plausible time rather than the zero value.
	last time.Time
}

// NewClock builds a clock over a recording's timestamps.
func NewClock(rec *Recording) *Clock {
	c := &Clock{times: rec.Times}
	if len(rec.Times) > 0 {
		c.last = rec.Times[len(rec.Times)-1]
	}
	return c
}

// Now returns the next recorded timestamp.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.i < len(c.times) {
		t := c.times[c.i]
		c.i++
		c.last = t
		return t
	}
	return c.last
}

// Exhausted reports whether the replay asked for more times than the original
// recorded -- a weak signal that it is doing more work.
func (c *Clock) Exhausted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.i >= len(c.times)
}

// ---------------------------------------------------------------------------

func textOf(raw json.RawMessage) string {
	var probe struct {
		Text    string `json:"text"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return ""
	}
	if probe.Text != "" {
		return probe.Text
	}
	var out string
	for _, b := range probe.Content {
		if b.Type == "text" {
			out += b.Text
		}
	}
	return out
}

func toolCallOf(raw json.RawMessage) *provider.ToolCall {
	var probe struct {
		Tool    string `json:"tool"`
		Content []struct {
			Type  string          `json:"type"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil
	}
	for _, b := range probe.Content {
		if b.Type == "tool_use" {
			return &provider.ToolCall{Name: b.Name, Input: b.Input}
		}
	}
	if probe.Tool != "" {
		return &provider.ToolCall{Name: probe.Tool, Input: json.RawMessage(`{}`)}
	}
	return nil
}
