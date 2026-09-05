package journal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// RunStatus mirrors runs.status.
type RunStatus string

const (
	RunStatusPending     RunStatus = "pending"
	RunStatusRunning     RunStatus = "running"
	RunStatusCompleted   RunStatus = "completed"
	RunStatusFailed      RunStatus = "failed"
	RunStatusCancelled   RunStatus = "cancelled"
	RunStatusNeedsReview RunStatus = "needs_review"
)

// StepStatus mirrors steps.status.
type StepStatus string

const (
	StepStatusScheduled   StepStatus = "scheduled"
	StepStatusRunning     StepStatus = "running"
	StepStatusCompleted   StepStatus = "completed"
	StepStatusFailed      StepStatus = "failed"
	StepStatusRetrying    StepStatus = "retrying"
	StepStatusNeedsReview StepStatus = "needs_review"
)

// StepState is the folded state of one execution record.
//
// It carries the recorded provider response and tool output, not just metadata.
// That is deliberate: replay is this same fold plus a source that answers from
// these recorded values instead of the network, so the fold is the only place
// that needs to know how a run's non-determinism was captured.
type StepState struct {
	StepIndex      int        `json:"step_index"`
	Kind           StepKind   `json:"kind"`
	Name           string     `json:"name"`
	NodeID         string     `json:"node_id"`
	IdempotencyKey string     `json:"idempotency_key"`
	Status         StepStatus `json:"status"`
	Attempt        int        `json:"attempt"`
	RandSeed       int64      `json:"rand_seed"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Tokens         int64      `json:"tokens"`
	Cents          int64      `json:"cents"`
	Error          string     `json:"error,omitempty"`

	// Recorded non-determinism. Exactly one of these is populated on a
	// completed step, according to Kind.
	ModelResponse json.RawMessage `json:"model_response,omitempty"`
	ToolOutput    json.RawMessage `json:"tool_output,omitempty"`
	ToolExitCode  int             `json:"tool_exit_code,omitempty"`

	// RetryDelaysMS records every backoff this step waited, in order. Replay
	// skips the sleeps but must reproduce the sequence.
	RetryDelaysMS []int64 `json:"retry_delays_ms,omitempty"`
}

// RunState is the complete state of a run, derived only from its events.
//
// Nothing outside this struct is authoritative. The runs and steps tables hold
// a copy of these fields for query speed; anchorctl rebuild proves they agree.
type RunState struct {
	RunID        uuid.UUID       `json:"run_id"`
	TenantID     string          `json:"tenant_id"`
	AgentName    string          `json:"agent_name"`
	AgentVersion int             `json:"agent_version"`
	Input        json.RawMessage `json:"input"`
	Status       RunStatus       `json:"status"`

	BudgetTokens int64 `json:"budget_tokens"`
	BudgetCents  int64 `json:"budget_cents"`
	TokensUsed   int64 `json:"tokens_used"`
	CentsUsed    int64 `json:"cents_used"`

	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	TerminalOutput json.RawMessage `json:"terminal_output,omitempty"`
	Error          string          `json:"error,omitempty"`
	ReplayOf       *uuid.UUID      `json:"replay_of,omitempty"`

	// WorkerID is the worker that most recently started the run.
	WorkerID string `json:"worker_id,omitempty"`

	// CancelRequested is set by RunCancelled. The worker reads it at every step
	// boundary; cancellation is cooperative, never a kill.
	CancelRequested bool `json:"cancel_requested"`

	// Steps is dense and ordered: Steps[i].StepIndex == i always holds, because
	// the worker assigns step_index sequentially as it walks the graph.
	Steps []*StepState `json:"steps"`

	// NextSeq is the seq a writer must use for the next append. It is the whole
	// of Anchor's concurrency control: the writer computes it from the state it
	// folded, and UNIQUE (run_id, seq) rejects the loser of any race.
	NextSeq int `json:"next_seq"`

	// LastEventAt is the created_at of the newest applied event. Agent code
	// that asks for the current time gets this, never time.Now(), so a replay
	// observes the same clock the original run did.
	LastEventAt time.Time `json:"last_event_at"`
}

// Fold replays a run's events in order and returns the resulting state.
//
// events must be ordered by seq and must be the complete prefix of the run
// starting at seq 0. Fold is strict about this: a gap, a repeat, or a mismatched
// run_id is a corrupt log, and returning a plausible-looking state from a
// corrupt log is far worse than failing loudly.
func Fold(events []Event) (*RunState, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("fold: no events")
	}
	s := &RunState{}
	for _, e := range events {
		if err := s.Apply(e); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Apply folds a single event into the state. A worker holding a run keeps its
// RunState and applies each event as it appends it, so the in-memory state and
// the log never diverge.
func (s *RunState) Apply(e Event) error {
	if err := s.checkEnvelope(e); err != nil {
		return err
	}

	var err error
	switch e.Type {
	case TypeRunSubmitted:
		err = s.applyRunSubmitted(e)
	case TypeRunStarted:
		err = s.applyRunStarted(e)
	case TypeStepScheduled:
		err = s.applyStepScheduled(e)
	case TypeModelCallStarted:
		err = s.applyModelCallStarted(e)
	case TypeModelCallCompleted:
		err = s.applyModelCallCompleted(e)
	case TypeToolCallStarted:
		err = s.applyToolCallStarted(e)
	case TypeToolCallCompleted:
		err = s.applyToolCallCompleted(e)
	case TypeStepFailed:
		err = s.applyStepFailed(e)
	case TypeRetryScheduled:
		err = s.applyRetryScheduled(e)
	case TypeBudgetExceeded:
		err = s.applyBudgetExceeded(e)
	case TypeRunCancelled:
		err = s.applyRunCancelled(e)
	case TypeRunCompleted:
		err = s.applyRunCompleted(e)
	case TypeRunFailed:
		err = s.applyRunFailed(e)
	default:
		// An unknown type means this build is older than the log. Guessing
		// would produce a state that silently omits whatever that event did.
		err = fmt.Errorf("unknown event type %q", e.Type)
	}
	if err != nil {
		return fmt.Errorf("seq %d (%s): %w", e.Seq, e.Type, err)
	}

	s.NextSeq = e.Seq + 1
	s.LastEventAt = e.CreatedAt
	return nil
}

// checkEnvelope enforces the invariants that hold for every event regardless of
// type: correct run, correct position, and no writes past a terminal state.
func (s *RunState) checkEnvelope(e Event) error {
	if s.NextSeq == 0 {
		if e.Type != TypeRunSubmitted {
			return fmt.Errorf("first event must be %s, got %s", TypeRunSubmitted, e.Type)
		}
	} else {
		if e.Type == TypeRunSubmitted {
			return fmt.Errorf("seq %d: %s may only appear at seq 0", e.Seq, TypeRunSubmitted)
		}
		if e.RunID != s.RunID {
			return fmt.Errorf("seq %d: event belongs to run %s, folding run %s", e.Seq, e.RunID, s.RunID)
		}
	}

	if e.Seq != s.NextSeq {
		return fmt.Errorf("out-of-order event: expected seq %d, got %d (%s)", s.NextSeq, e.Seq, e.Type)
	}

	// RunCompleted and RunFailed are hard-terminal: nothing may follow them.
	// RunCancelled is soft-terminal -- an in-flight step was aborted by the
	// worker's context and its StepFailed still needs somewhere to land -- so it
	// admits step failures but no new work.
	switch s.Status {
	case RunStatusCompleted, RunStatusFailed, RunStatusNeedsReview:
		return fmt.Errorf("run already terminal (%s); cannot apply %s", s.Status, e.Type)
	case RunStatusCancelled:
		switch e.Type {
		case TypeStepFailed, TypeRunFailed:
			// permitted: the aborted in-flight step reports itself
		default:
			return fmt.Errorf("run cancelled; cannot apply %s", e.Type)
		}
	}
	return nil
}

func (s *RunState) applyRunSubmitted(e Event) error {
	p, err := decodePayload[RunSubmittedPayload](e.Payload)
	if err != nil {
		return err
	}
	if p.TenantID == "" {
		return fmt.Errorf("tenant_id is required")
	}
	if p.AgentName == "" {
		return fmt.Errorf("agent_name is required")
	}
	s.RunID = e.RunID
	s.TenantID = p.TenantID
	s.AgentName = p.AgentName
	s.AgentVersion = p.AgentVersion
	s.Input = p.Input
	s.BudgetTokens = p.BudgetTokens
	s.BudgetCents = p.BudgetCents
	s.ReplayOf = p.ReplayOf
	s.CreatedAt = e.CreatedAt
	s.Status = RunStatusPending
	return nil
}

func (s *RunState) applyRunStarted(e Event) error {
	p, err := decodePayload[RunStartedPayload](e.Payload)
	if err != nil {
		return err
	}
	s.WorkerID = p.WorkerID
	s.Status = RunStatusRunning
	// StartedAt is the FIRST start. A lease takeover writes another RunStarted
	// with Resumed set; the run did not start over, it resumed.
	if s.StartedAt == nil {
		t := e.CreatedAt
		s.StartedAt = &t
	}
	return nil
}

func (s *RunState) applyStepScheduled(e Event) error {
	p, err := decodePayload[StepScheduledPayload](e.Payload)
	if err != nil {
		return err
	}
	// step_index is assigned by the worker as it walks the graph and must be
	// dense and monotonic. A hole would make Steps[i] ambiguous and would break
	// the idempotency key, which is derived from the index.
	if p.StepIndex != len(s.Steps) {
		return fmt.Errorf("step_index %d is not the next index %d", p.StepIndex, len(s.Steps))
	}
	if p.Kind != StepKindModel && p.Kind != StepKindTool {
		return fmt.Errorf("invalid step kind %q", p.Kind)
	}
	if p.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	t := e.CreatedAt
	s.Steps = append(s.Steps, &StepState{
		StepIndex:      p.StepIndex,
		Kind:           p.Kind,
		Name:           p.Name,
		NodeID:         p.NodeID,
		IdempotencyKey: p.IdempotencyKey,
		RandSeed:       p.RandSeed,
		Status:         StepStatusScheduled,
		Attempt:        0,
		StartedAt:      &t,
	})
	return nil
}

func (s *RunState) applyModelCallStarted(e Event) error {
	p, err := decodePayload[ModelCallStartedPayload](e.Payload)
	if err != nil {
		return err
	}
	st, err := s.stepForUpdate(p.StepIndex, StepKindModel, p.Attempt)
	if err != nil {
		return err
	}
	st.Status = StepStatusRunning
	return nil
}

func (s *RunState) applyModelCallCompleted(e Event) error {
	p, err := decodePayload[ModelCallCompletedPayload](e.Payload)
	if err != nil {
		return err
	}
	st, err := s.stepForUpdate(p.StepIndex, StepKindModel, p.Attempt)
	if err != nil {
		return err
	}
	st.Status = StepStatusCompleted
	st.ModelResponse = p.Response
	st.Tokens = p.TotalTokens()
	st.Cents = p.Cents
	t := e.CreatedAt
	st.FinishedAt = &t

	// Budget accounting lives in the fold so it is identical everywhere: the
	// worker's live check, the rebuilt projection, and the trace viewer all get
	// the same number from the same code.
	s.TokensUsed += p.TotalTokens()
	s.CentsUsed += p.Cents
	return nil
}

func (s *RunState) applyToolCallStarted(e Event) error {
	p, err := decodePayload[ToolCallStartedPayload](e.Payload)
	if err != nil {
		return err
	}
	st, err := s.stepForUpdate(p.StepIndex, StepKindTool, p.Attempt)
	if err != nil {
		return err
	}
	st.Status = StepStatusRunning
	return nil
}

func (s *RunState) applyToolCallCompleted(e Event) error {
	p, err := decodePayload[ToolCallCompletedPayload](e.Payload)
	if err != nil {
		return err
	}
	st, err := s.stepForUpdate(p.StepIndex, StepKindTool, p.Attempt)
	if err != nil {
		return err
	}
	st.Status = StepStatusCompleted
	st.ToolOutput = p.Output
	st.ToolExitCode = p.ExitCode
	t := e.CreatedAt
	st.FinishedAt = &t
	return nil
}

func (s *RunState) applyStepFailed(e Event) error {
	p, err := decodePayload[StepFailedPayload](e.Payload)
	if err != nil {
		return err
	}
	st, err := s.step(p.StepIndex)
	if err != nil {
		return err
	}
	if p.Attempt != st.Attempt {
		return fmt.Errorf("step %d: failure for attempt %d but step is on attempt %d", p.StepIndex, p.Attempt, st.Attempt)
	}
	st.Error = p.Error
	t := e.CreatedAt
	st.FinishedAt = &t
	if p.NeedsReview {
		// A non-idempotent tool died in the ambiguous window. Anchor will not
		// guess whether the effect landed.
		st.Status = StepStatusNeedsReview
	} else {
		st.Status = StepStatusFailed
	}
	return nil
}

func (s *RunState) applyRetryScheduled(e Event) error {
	p, err := decodePayload[RetryScheduledPayload](e.Payload)
	if err != nil {
		return err
	}
	st, err := s.step(p.StepIndex)
	if err != nil {
		return err
	}
	if st.Status != StepStatusFailed {
		return fmt.Errorf("step %d: retry scheduled from status %q, expected %q", p.StepIndex, st.Status, StepStatusFailed)
	}
	if p.Attempt != st.Attempt+1 {
		return fmt.Errorf("step %d: retry attempt %d does not follow attempt %d", p.StepIndex, p.Attempt, st.Attempt)
	}
	st.Attempt = p.Attempt
	st.Status = StepStatusRetrying
	st.Error = ""
	st.FinishedAt = nil
	st.RetryDelaysMS = append(st.RetryDelaysMS, p.DelayMS)
	return nil
}

func (s *RunState) applyBudgetExceeded(e Event) error {
	p, err := decodePayload[BudgetExceededPayload](e.Payload)
	if err != nil {
		return err
	}
	if p.Resource != "tokens" && p.Resource != "cents" {
		return fmt.Errorf("invalid budget resource %q", p.Resource)
	}
	// BudgetExceeded records why the run is about to die but is not itself
	// terminal; the worker follows it immediately with RunFailed. Keeping every
	// terminal transition in one small set of event types is what lets the fold
	// answer "how did this run end" without special cases.
	s.Error = fmt.Sprintf("budget exceeded: %s limit %d, used %d, projected %d",
		p.Resource, p.Limit, p.Used, p.Projected)
	return nil
}

func (s *RunState) applyRunCancelled(e Event) error {
	p, err := decodePayload[RunCancelledPayload](e.Payload)
	if err != nil {
		return err
	}
	s.CancelRequested = true
	s.Status = RunStatusCancelled
	t := e.CreatedAt
	s.FinishedAt = &t
	if p.Reason != "" {
		s.Error = p.Reason
	}
	return nil
}

func (s *RunState) applyRunCompleted(e Event) error {
	p, err := decodePayload[RunCompletedPayload](e.Payload)
	if err != nil {
		return err
	}
	s.Status = RunStatusCompleted
	s.TerminalOutput = p.Output
	t := e.CreatedAt
	s.FinishedAt = &t
	return nil
}

func (s *RunState) applyRunFailed(e Event) error {
	p, err := decodePayload[RunFailedPayload](e.Payload)
	if err != nil {
		return err
	}
	if p.NeedsReview {
		s.Status = RunStatusNeedsReview
	} else {
		s.Status = RunStatusFailed
	}
	if p.Error != "" {
		s.Error = p.Error
	}
	t := e.CreatedAt
	s.FinishedAt = &t
	return nil
}

// step returns the step at index i.
func (s *RunState) step(i int) (*StepState, error) {
	if i < 0 || i >= len(s.Steps) {
		return nil, fmt.Errorf("step %d was never scheduled (run has %d steps)", i, len(s.Steps))
	}
	return s.Steps[i], nil
}

// stepForUpdate fetches a step and checks that the event matches its kind and
// current attempt. A mismatch means an event from a superseded attempt arrived,
// which would corrupt token accounting if applied.
func (s *RunState) stepForUpdate(i int, kind StepKind, attempt int) (*StepState, error) {
	st, err := s.step(i)
	if err != nil {
		return nil, err
	}
	if st.Kind != kind {
		return nil, fmt.Errorf("step %d is a %s step, got a %s event", i, st.Kind, kind)
	}
	if attempt != st.Attempt {
		return nil, fmt.Errorf("step %d: event for attempt %d but step is on attempt %d", i, attempt, st.Attempt)
	}
	return st, nil
}

// Step exposes a step by index for callers outside the package.
func (s *RunState) Step(i int) (*StepState, bool) {
	if i < 0 || i >= len(s.Steps) {
		return nil, false
	}
	return s.Steps[i], true
}

// Terminal reports whether the run has reached a state no worker will advance.
func (s *RunState) Terminal() bool {
	switch s.Status {
	case RunStatusCompleted, RunStatusFailed, RunStatusCancelled, RunStatusNeedsReview:
		return true
	}
	return false
}

// Now returns the run's notion of the current time: the timestamp of the last
// applied event. Agent-visible code must call this rather than time.Now(), so a
// replay observes exactly the clock the original run observed.
func (s *RunState) Now() time.Time { return s.LastEventAt }

// Clone returns a deep copy of the state.
//
// The append path folds candidate events into a clone first: if a candidate
// would corrupt the state, the caller finds out before anything reaches the
// database, and the live state the worker is holding is left untouched.
func (s *RunState) Clone() *RunState {
	if s == nil {
		return nil
	}
	c := *s
	// Preserve nil-ness rather than normalising to an empty slice. A run with
	// no steps folds to a nil Steps, and a clone that turned it into []
	// would make the writer's state and the same run folded from the log
	// unequal -- identical in meaning, different on the wire.
	if s.Steps != nil {
		c.Steps = make([]*StepState, len(s.Steps))
		for i, st := range s.Steps {
			cp := *st
			if st.RetryDelaysMS != nil {
				cp.RetryDelaysMS = append([]int64(nil), st.RetryDelaysMS...)
			}
			c.Steps[i] = &cp
		}
	}
	// Times are values behind pointers; copy them so a mutation of the clone
	// cannot reach back into the original.
	c.StartedAt = clonePtr(s.StartedAt)
	c.FinishedAt = clonePtr(s.FinishedAt)
	for i, st := range s.Steps {
		c.Steps[i].StartedAt = clonePtr(st.StartedAt)
		c.Steps[i].FinishedAt = clonePtr(st.FinishedAt)
	}
	return &c
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
