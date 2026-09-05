package journal

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// baseTime is fixed so every assertion about timestamps is exact. Tests that
// depend on time.Now() are exactly the kind of non-determinism this project
// exists to eliminate.
var baseTime = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// logBuilder assembles a well-formed event log. Each event advances the clock
// by one second so ordering assertions are readable.
type logBuilder struct {
	t     *testing.T
	runID uuid.UUID
	seq   int
	out   []Event
}

func newLog(t *testing.T) *logBuilder {
	t.Helper()
	return &logBuilder{t: t, runID: uuid.MustParse("11111111-1111-4111-8111-111111111111")}
}

// add appends an event with the next seq. It marshals through MarshalPayload so
// a payload that forgets its version fails the test rather than the log.
func (b *logBuilder) add(typ EventType, p any) *logBuilder {
	b.t.Helper()
	raw, err := MarshalPayload(p)
	if err != nil {
		b.t.Fatalf("marshal %s: %v", typ, err)
	}
	b.out = append(b.out, Event{
		ID:        int64(b.seq + 1),
		RunID:     b.runID,
		Seq:       b.seq,
		Type:      typ,
		Payload:   raw,
		CreatedAt: baseTime.Add(time.Duration(b.seq) * time.Second),
	})
	b.seq++
	return b
}

// addRaw appends an event with a hand-written payload, for version and
// corruption cases that the typed structs would not let us express.
func (b *logBuilder) addRaw(typ EventType, payload string) *logBuilder {
	b.t.Helper()
	b.out = append(b.out, Event{
		ID:        int64(b.seq + 1),
		RunID:     b.runID,
		Seq:       b.seq,
		Type:      typ,
		Payload:   json.RawMessage(payload),
		CreatedAt: baseTime.Add(time.Duration(b.seq) * time.Second),
	})
	b.seq++
	return b
}

func (b *logBuilder) events() []Event { return b.out }

func v() Versioned { return Versioned{V: CurrentPayloadVersion} }

// submitted returns a builder with the opening event already applied.
func submitted(t *testing.T) *logBuilder {
	return newLog(t).add(TypeRunSubmitted, RunSubmittedPayload{
		Versioned:    v(),
		TenantID:     "acme",
		AgentName:    "research",
		AgentVersion: 1,
		Input:        json.RawMessage(`{"q":"why is the sky blue"}`),
		BudgetTokens: 100000,
		BudgetCents:  500,
	})
}

// started returns a builder that has been submitted and claimed by a worker.
func started(t *testing.T) *logBuilder {
	return submitted(t).add(TypeRunStarted, RunStartedPayload{Versioned: v(), WorkerID: "w-1"})
}

func mustFold(t *testing.T, events []Event) *RunState {
	t.Helper()
	s, err := Fold(events)
	if err != nil {
		t.Fatalf("Fold: unexpected error: %v", err)
	}
	return s
}

// foldError asserts the fold rejects the log and that the message mentions want.
func foldError(t *testing.T, events []Event, want string) {
	t.Helper()
	_, err := Fold(events)
	if err == nil {
		t.Fatalf("Fold: expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Fold: error %q does not contain %q", err.Error(), want)
	}
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

func TestFoldCompleteModelRun(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, Name: "answer",
			NodeID: "n1", IdempotencyKey: "k0", RandSeed: 42,
		}).
		add(TypeModelCallStarted, ModelCallStartedPayload{
			Versioned: v(), StepIndex: 0, Model: "claude-sonnet-4-6",
			Request: json.RawMessage(`{"messages":[]}`),
		}).
		add(TypeModelCallCompleted, ModelCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Model: "claude-sonnet-4-6",
			Response:     json.RawMessage(`{"text":"rayleigh scattering"}`),
			PromptTokens: 120, CompletionTokens: 80, Cents: 3,
		}).
		add(TypeRunCompleted, RunCompletedPayload{
			Versioned: v(), Output: json.RawMessage(`{"answer":"rayleigh scattering"}`),
		})

	s := mustFold(t, b.events())

	if s.Status != RunStatusCompleted {
		t.Errorf("status = %q, want %q", s.Status, RunStatusCompleted)
	}
	if s.TenantID != "acme" || s.AgentName != "research" || s.AgentVersion != 1 {
		t.Errorf("identity fields wrong: %+v", s)
	}
	if s.TokensUsed != 200 {
		t.Errorf("TokensUsed = %d, want 200", s.TokensUsed)
	}
	if s.CentsUsed != 3 {
		t.Errorf("CentsUsed = %d, want 3", s.CentsUsed)
	}
	if len(s.Steps) != 1 {
		t.Fatalf("len(Steps) = %d, want 1", len(s.Steps))
	}
	st := s.Steps[0]
	if st.Status != StepStatusCompleted {
		t.Errorf("step status = %q, want %q", st.Status, StepStatusCompleted)
	}
	if st.Tokens != 200 || st.Cents != 3 {
		t.Errorf("step accounting = %d tokens %d cents, want 200/3", st.Tokens, st.Cents)
	}
	if st.RandSeed != 42 {
		t.Errorf("RandSeed = %d, want 42", st.RandSeed)
	}
	if string(st.ModelResponse) != `{"text":"rayleigh scattering"}` {
		t.Errorf("recorded response not preserved: %s", st.ModelResponse)
	}
	if s.NextSeq != 6 {
		t.Errorf("NextSeq = %d, want 6", s.NextSeq)
	}
	if !s.Terminal() {
		t.Error("Terminal() = false, want true")
	}
	// StartedAt is the RunStarted timestamp (seq 1), FinishedAt the last event.
	if s.StartedAt == nil || !s.StartedAt.Equal(baseTime.Add(1*time.Second)) {
		t.Errorf("StartedAt = %v, want %v", s.StartedAt, baseTime.Add(1*time.Second))
	}
	if s.FinishedAt == nil || !s.FinishedAt.Equal(baseTime.Add(5*time.Second)) {
		t.Errorf("FinishedAt = %v, want %v", s.FinishedAt, baseTime.Add(5*time.Second))
	}
}

func TestFoldToolStepRecordsOutput(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindTool, Name: "web_fetch",
			IdempotencyKey: "k0",
		}).
		add(TypeToolCallStarted, ToolCallStartedPayload{
			Versioned: v(), StepIndex: 0, Tool: "web_fetch",
			Input: json.RawMessage(`{"url":"https://example.com"}`),
		}).
		add(TypeToolCallCompleted, ToolCallCompletedPayload{
			Versioned: v(), StepIndex: 0, ExitCode: 0,
			Output: json.RawMessage(`{"body":"hello"}`),
		})

	s := mustFold(t, b.events())
	st := s.Steps[0]
	if st.Status != StepStatusCompleted {
		t.Errorf("status = %q, want completed", st.Status)
	}
	if string(st.ToolOutput) != `{"body":"hello"}` {
		t.Errorf("tool output = %s", st.ToolOutput)
	}
	// Tool steps cost no tokens; only model calls move the budget.
	if s.TokensUsed != 0 || s.CentsUsed != 0 {
		t.Errorf("tool step moved the budget: %d tokens %d cents", s.TokensUsed, s.CentsUsed)
	}
}

func TestFoldAccumulatesAcrossSteps(t *testing.T) {
	b := started(t)
	for i := range 3 {
		b.add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: i, Kind: StepKindModel,
			Name: "loop", IdempotencyKey: string(rune('a' + i)),
		}).
			add(TypeModelCallStarted, ModelCallStartedPayload{Versioned: v(), StepIndex: i, Model: "m"}).
			add(TypeModelCallCompleted, ModelCallCompletedPayload{
				Versioned: v(), StepIndex: i, Model: "m",
				PromptTokens: 10, CompletionTokens: 5, Cents: 2,
			})
	}
	s := mustFold(t, b.events())
	if s.TokensUsed != 45 {
		t.Errorf("TokensUsed = %d, want 45", s.TokensUsed)
	}
	if s.CentsUsed != 6 {
		t.Errorf("CentsUsed = %d, want 6", s.CentsUsed)
	}
	if len(s.Steps) != 3 {
		t.Errorf("len(Steps) = %d, want 3", len(s.Steps))
	}
}

// ---------------------------------------------------------------------------
// Log integrity -- the fold must refuse a corrupt log rather than invent state
// ---------------------------------------------------------------------------

func TestFoldRejectsEmptyLog(t *testing.T) {
	foldError(t, nil, "no events")
}

func TestFoldRejectsFirstEventNotRunSubmitted(t *testing.T) {
	b := newLog(t).add(TypeRunStarted, RunStartedPayload{Versioned: v(), WorkerID: "w-1"})
	foldError(t, b.events(), "first event must be RunSubmitted")
}

func TestFoldRejectsSecondRunSubmitted(t *testing.T) {
	b := submitted(t).add(TypeRunSubmitted, RunSubmittedPayload{
		Versioned: v(), TenantID: "acme", AgentName: "research",
	})
	foldError(t, b.events(), "may only appear at seq 0")
}

func TestFoldRejectsSeqGap(t *testing.T) {
	events := started(t).events()
	events[1].Seq = 5 // a hole in the log
	foldError(t, events, "expected seq 1, got 5")
}

func TestFoldRejectsRepeatedSeq(t *testing.T) {
	events := started(t).events()
	events[1].Seq = 0
	foldError(t, events, "expected seq 1, got 0")
}

func TestFoldRejectsForeignRunID(t *testing.T) {
	events := started(t).events()
	events[1].RunID = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	foldError(t, events, "folding run")
}

func TestFoldRejectsUnknownEventType(t *testing.T) {
	b := submitted(t).addRaw("SomethingFromTheFuture", `{"v":1}`)
	foldError(t, b.events(), "unknown event type")
}

func TestFoldRejectsEventsAfterTerminal(t *testing.T) {
	b := started(t).
		add(TypeRunCompleted, RunCompletedPayload{Versioned: v(), Output: json.RawMessage(`{}`)}).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k",
		})
	foldError(t, b.events(), "run already terminal")
}

// ---------------------------------------------------------------------------
// Step indexing
// ---------------------------------------------------------------------------

func TestFoldRejectsNonDenseStepIndex(t *testing.T) {
	b := started(t).add(TypeStepScheduled, StepScheduledPayload{
		Versioned: v(), StepIndex: 3, Kind: StepKindModel, IdempotencyKey: "k",
	})
	foldError(t, b.events(), "is not the next index")
}

func TestFoldRejectsMissingIdempotencyKey(t *testing.T) {
	b := started(t).add(TypeStepScheduled, StepScheduledPayload{
		Versioned: v(), StepIndex: 0, Kind: StepKindModel,
	})
	foldError(t, b.events(), "idempotency_key is required")
}

func TestFoldRejectsEventForUnscheduledStep(t *testing.T) {
	b := started(t).add(TypeModelCallStarted, ModelCallStartedPayload{
		Versioned: v(), StepIndex: 0, Model: "m",
	})
	foldError(t, b.events(), "was never scheduled")
}

func TestFoldRejectsKindMismatch(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k",
		}).
		add(TypeToolCallStarted, ToolCallStartedPayload{Versioned: v(), StepIndex: 0, Tool: "t"})
	foldError(t, b.events(), "is a model step, got a tool event")
}

// ---------------------------------------------------------------------------
// Retries
// ---------------------------------------------------------------------------

func TestFoldRetrySucceedsOnSecondAttempt(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0", RandSeed: 7,
		}).
		add(TypeModelCallStarted, ModelCallStartedPayload{Versioned: v(), StepIndex: 0, Attempt: 0, Model: "m"}).
		add(TypeStepFailed, StepFailedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 0, Error: "provider 503", Retryable: true,
		}).
		add(TypeRetryScheduled, RetryScheduledPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, DelayMS: 250,
		}).
		add(TypeModelCallStarted, ModelCallStartedPayload{Versioned: v(), StepIndex: 0, Attempt: 1, Model: "m"}).
		add(TypeModelCallCompleted, ModelCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, Model: "m",
			PromptTokens: 10, CompletionTokens: 10, Cents: 1,
		})

	s := mustFold(t, b.events())
	st := s.Steps[0]
	if st.Status != StepStatusCompleted {
		t.Errorf("status = %q, want completed", st.Status)
	}
	if st.Attempt != 1 {
		t.Errorf("Attempt = %d, want 1", st.Attempt)
	}
	if st.Error != "" {
		t.Errorf("Error = %q, want cleared after successful retry", st.Error)
	}
	// A retry is the same logical step: one step, one seed, one idempotency key.
	if len(s.Steps) != 1 {
		t.Errorf("retry created a new step: len(Steps) = %d", len(s.Steps))
	}
	if st.RandSeed != 7 {
		t.Errorf("retry changed the seed: %d", st.RandSeed)
	}
	if got := st.RetryDelaysMS; len(got) != 1 || got[0] != 250 {
		t.Errorf("RetryDelaysMS = %v, want [250]", got)
	}
	// Only the succeeding attempt is billed.
	if s.TokensUsed != 20 {
		t.Errorf("TokensUsed = %d, want 20", s.TokensUsed)
	}
}

func TestFoldRejectsRetryFromNonFailedStep(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
		}).
		add(TypeRetryScheduled, RetryScheduledPayload{Versioned: v(), StepIndex: 0, Attempt: 1})
	foldError(t, b.events(), "retry scheduled from status")
}

func TestFoldRejectsAttemptSkip(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
		}).
		add(TypeStepFailed, StepFailedPayload{Versioned: v(), StepIndex: 0, Attempt: 0, Retryable: true}).
		add(TypeRetryScheduled, RetryScheduledPayload{Versioned: v(), StepIndex: 0, Attempt: 3})
	foldError(t, b.events(), "does not follow attempt")
}

// A late event from a superseded attempt must not be applied -- applying it
// would double-count tokens for a step that already moved on.
func TestFoldRejectsEventFromStaleAttempt(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
		}).
		add(TypeStepFailed, StepFailedPayload{Versioned: v(), StepIndex: 0, Attempt: 0, Retryable: true}).
		add(TypeRetryScheduled, RetryScheduledPayload{Versioned: v(), StepIndex: 0, Attempt: 1}).
		add(TypeModelCallCompleted, ModelCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 0, Model: "m", PromptTokens: 99,
		})
	foldError(t, b.events(), "attempt 0 but step is on attempt 1")
}

// ---------------------------------------------------------------------------
// Cancellation, budgets, review
// ---------------------------------------------------------------------------

func TestFoldCancellationAdmitsAbortedStepFailure(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
		}).
		add(TypeModelCallStarted, ModelCallStartedPayload{Versioned: v(), StepIndex: 0, Model: "m"}).
		add(TypeRunCancelled, RunCancelledPayload{
			Versioned: v(), Reason: "user cancelled", RequestedBy: "api",
		}).
		// The in-flight call was aborted through the worker's context; its
		// failure still needs to land in the log.
		add(TypeStepFailed, StepFailedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 0, Error: "context canceled",
		})

	s := mustFold(t, b.events())
	if s.Status != RunStatusCancelled {
		t.Errorf("status = %q, want cancelled", s.Status)
	}
	if !s.CancelRequested {
		t.Error("CancelRequested = false, want true")
	}
	if s.Steps[0].Status != StepStatusFailed {
		t.Errorf("aborted step status = %q, want failed", s.Steps[0].Status)
	}
}

func TestFoldCancellationRejectsNewWork(t *testing.T) {
	b := started(t).
		add(TypeRunCancelled, RunCancelledPayload{Versioned: v(), Reason: "user cancelled"}).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
		})
	foldError(t, b.events(), "run cancelled; cannot apply StepScheduled")
}

func TestFoldBudgetExceededThenFailed(t *testing.T) {
	b := started(t).
		add(TypeBudgetExceeded, BudgetExceededPayload{
			Versioned: v(), Resource: "tokens", Limit: 100000, Used: 99000, Projected: 104000,
		}).
		add(TypeRunFailed, RunFailedPayload{Versioned: v(), Error: "budget exceeded"})

	s := mustFold(t, b.events())
	if s.Status != RunStatusFailed {
		t.Errorf("status = %q, want failed", s.Status)
	}
	if !strings.Contains(s.Error, "budget exceeded") {
		t.Errorf("Error = %q, want it to mention the budget", s.Error)
	}
}

func TestFoldNeedsReviewOnAmbiguousNonIdempotentTool(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindTool, Name: "charge_card",
			IdempotencyKey: "k0",
		}).
		add(TypeToolCallStarted, ToolCallStartedPayload{Versioned: v(), StepIndex: 0, Tool: "charge_card"}).
		add(TypeStepFailed, StepFailedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 0,
			Error:       "worker died mid-call; effect may or may not have landed",
			Retryable:   false,
			NeedsReview: true,
		}).
		add(TypeRunFailed, RunFailedPayload{
			Versioned: v(), Error: "halted for review", NeedsReview: true,
		})

	s := mustFold(t, b.events())
	if s.Status != RunStatusNeedsReview {
		t.Errorf("status = %q, want needs_review", s.Status)
	}
	if s.Steps[0].Status != StepStatusNeedsReview {
		t.Errorf("step status = %q, want needs_review", s.Steps[0].Status)
	}
}

// ---------------------------------------------------------------------------
// Lease takeover
// ---------------------------------------------------------------------------

func TestFoldResumeKeepsOriginalStartedAt(t *testing.T) {
	b := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
		}).
		// Worker 1 was SIGKILLed here. Worker 2 takes the expired lease.
		add(TypeRunStarted, RunStartedPayload{Versioned: v(), WorkerID: "w-2", Resumed: true})

	s := mustFold(t, b.events())
	if s.WorkerID != "w-2" {
		t.Errorf("WorkerID = %q, want w-2 after takeover", s.WorkerID)
	}
	if s.StartedAt == nil || !s.StartedAt.Equal(baseTime.Add(1*time.Second)) {
		t.Errorf("StartedAt = %v, want the ORIGINAL start; a resume is not a restart", s.StartedAt)
	}
	if s.Status != RunStatusRunning {
		t.Errorf("status = %q, want running", s.Status)
	}
}

// ---------------------------------------------------------------------------
// Payload versioning -- a run recorded under an older schema must still fold
// ---------------------------------------------------------------------------

func TestFoldRejectsPayloadFromNewerBuild(t *testing.T) {
	b := submitted(t).addRaw(TypeRunStarted, `{"v":99,"worker_id":"w-1"}`)
	foldError(t, b.events(), "newer than this build understands")
}

func TestFoldRejectsUnversionedPayload(t *testing.T) {
	b := submitted(t).addRaw(TypeRunStarted, `{"worker_id":"w-1"}`)
	foldError(t, b.events(), "missing its \"v\" version field")
}

// A payload written by an older build has fewer fields than the current struct.
// The fold must read it without complaint and leave the new fields at zero.
func TestFoldAcceptsSparseOlderPayload(t *testing.T) {
	b := newLog(t).
		addRaw(TypeRunSubmitted, `{"v":1,"tenant_id":"acme","agent_name":"research","input":{}}`).
		addRaw(TypeRunStarted, `{"v":1,"worker_id":"w-1"}`)

	s := mustFold(t, b.events())
	if s.TenantID != "acme" || s.AgentName != "research" {
		t.Errorf("older payload did not fold: %+v", s)
	}
	if s.BudgetTokens != 0 || s.ReplayOf != nil {
		t.Errorf("absent fields should stay zero, got %d / %v", s.BudgetTokens, s.ReplayOf)
	}
}

// A payload written by a LATER build of the same version carries fields this
// build does not know. Ignoring them is correct; erroring would make every
// rolling deploy an outage.
func TestFoldIgnoresUnknownPayloadFields(t *testing.T) {
	b := newLog(t).
		addRaw(TypeRunSubmitted, `{"v":1,"tenant_id":"acme","agent_name":"research","input":{},"future_field":"ignored"}`)
	s := mustFold(t, b.events())
	if s.TenantID != "acme" {
		t.Errorf("unknown field broke the fold: %+v", s)
	}
}

func TestMarshalPayloadRejectsMissingVersion(t *testing.T) {
	// Versioned left at its zero value -- the mistake this guard exists to catch.
	_, err := MarshalPayload(RunStartedPayload{WorkerID: "w-1"})
	if err == nil {
		t.Fatal("MarshalPayload accepted an unversioned payload")
	}
	if !strings.Contains(err.Error(), "without a version") {
		t.Errorf("error = %q, want it to name the missing version", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

func TestRunStateNowIsRecordedNotWallClock(t *testing.T) {
	b := started(t)
	s := mustFold(t, b.events())
	want := baseTime.Add(1 * time.Second)
	if !s.Now().Equal(want) {
		t.Errorf("Now() = %v, want the last event's timestamp %v", s.Now(), want)
	}
	// If Now() ever reached for the wall clock this would be within seconds of
	// the test run rather than pinned to the fixed baseTime above.
	if time.Since(s.Now()) < time.Hour {
		t.Error("Now() appears to be reading the wall clock, not the journal")
	}
}

// Folding the same log twice must produce identical state. This is the
// property every other guarantee in Anchor rests on.
func TestFoldIsDeterministic(t *testing.T) {
	events := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0", RandSeed: 99,
		}).
		add(TypeModelCallStarted, ModelCallStartedPayload{Versioned: v(), StepIndex: 0, Model: "m"}).
		add(TypeModelCallCompleted, ModelCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Model: "m", PromptTokens: 3, CompletionTokens: 4, Cents: 1,
		}).
		add(TypeRunCompleted, RunCompletedPayload{Versioned: v(), Output: json.RawMessage(`{"ok":true}`)}).
		events()

	a := mustFold(t, events)
	c := mustFold(t, events)

	ja, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	jc, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(ja) != string(jc) {
		t.Errorf("fold is not deterministic:\n first: %s\nsecond: %s", ja, jc)
	}
}

// Applying events one at a time (what a worker does while it holds a run) must
// produce the same state as folding the whole log at once (what recovery does).
func TestIncrementalApplyMatchesFullFold(t *testing.T) {
	events := started(t).
		add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindTool, Name: "t", IdempotencyKey: "k0",
		}).
		add(TypeToolCallStarted, ToolCallStartedPayload{Versioned: v(), StepIndex: 0, Tool: "t"}).
		add(TypeToolCallCompleted, ToolCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Output: json.RawMessage(`{"n":1}`),
		}).
		events()

	full := mustFold(t, events)

	incremental := &RunState{}
	for _, e := range events {
		if err := incremental.Apply(e); err != nil {
			t.Fatalf("Apply(seq %d): %v", e.Seq, err)
		}
	}

	jf, _ := json.Marshal(full)
	ji, _ := json.Marshal(incremental)
	if string(jf) != string(ji) {
		t.Errorf("incremental apply diverged from full fold:\n  full: %s\n  incr: %s", jf, ji)
	}
}

// A clone must be indistinguishable from the original, including the
// difference between a nil slice and an empty one. The append path clones the
// state before folding candidate events into it, so a clone that normalised
// nil to [] would make a worker's in-memory state unequal to the same run
// folded from the log -- the same value, encoded differently.
func TestCloneIsIndistinguishableFromTheOriginal(t *testing.T) {
	cases := map[string][]Event{
		"no steps": started(t).events(),
		"one step": started(t).add(TypeStepScheduled, StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
		}).events(),
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			original := mustFold(t, events)
			clone := original.Clone()

			jo, err := json.Marshal(original)
			if err != nil {
				t.Fatalf("marshal original: %v", err)
			}
			jc, err := json.Marshal(clone)
			if err != nil {
				t.Fatalf("marshal clone: %v", err)
			}
			if string(jo) != string(jc) {
				t.Errorf("clone differs from original:\noriginal: %s\n   clone: %s", jo, jc)
			}
			if (original.Steps == nil) != (clone.Steps == nil) {
				t.Errorf("clone changed Steps nil-ness: original nil=%v, clone nil=%v",
					original.Steps == nil, clone.Steps == nil)
			}
		})
	}
}

// Mutating a clone must not reach back into the original -- the append path
// depends on the caller's state being untouched when an append fails.
func TestCloneIsDeep(t *testing.T) {
	original := mustFold(t, started(t).add(TypeStepScheduled, StepScheduledPayload{
		Versioned: v(), StepIndex: 0, Kind: StepKindModel, IdempotencyKey: "k0",
	}).events())

	clone := original.Clone()
	clone.Steps[0].Status = StepStatusCompleted
	clone.Steps[0].Tokens = 999
	clone.TokensUsed = 999
	if clone.StartedAt != nil {
		*clone.StartedAt = time.Unix(0, 0)
	}

	if original.Steps[0].Status != StepStatusScheduled {
		t.Errorf("mutating the clone changed the original's step status: %q", original.Steps[0].Status)
	}
	if original.Steps[0].Tokens != 0 || original.TokensUsed != 0 {
		t.Error("mutating the clone changed the original's accounting")
	}
	if original.StartedAt != nil && original.StartedAt.Equal(time.Unix(0, 0)) {
		t.Error("mutating the clone's StartedAt changed the original's")
	}
}
