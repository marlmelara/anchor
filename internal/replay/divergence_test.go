package replay_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/replay"
)

var base = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
var runA = uuid.MustParse("11111111-1111-4111-8111-111111111111")

// log builds an event sequence. Timestamps advance by a second, and a replay's
// log is built with DIFFERENT timestamps on purpose -- a replay happens later by
// definition, so the check must not compare them.
type log struct {
	t      *testing.T
	runID  uuid.UUID
	at     time.Time
	seq    int
	events []journal.Event
}

func newLog(t *testing.T, runID uuid.UUID, start time.Time) *log {
	return &log{t: t, runID: runID, at: start, seq: journal.FirstSeq}
}

func (l *log) add(typ journal.EventType, payload any) *log {
	l.t.Helper()
	raw, err := journal.MarshalPayload(payload)
	if err != nil {
		l.t.Fatalf("marshal %s: %v", typ, err)
	}
	l.events = append(l.events, journal.Event{
		RunID: l.runID, Seq: l.seq, Type: typ, Payload: raw, CreatedAt: l.at,
	})
	l.seq++
	l.at = l.at.Add(time.Second)
	return l
}

func v() journal.Versioned { return journal.Versioned{V: journal.CurrentPayloadVersion} }

// completeRun is one model call then one tool call, the shape most tests use.
func completeRun(t *testing.T, runID uuid.UUID, start time.Time) *log {
	return newLog(t, runID, start).
		add(journal.TypeRunSubmitted, journal.RunSubmittedPayload{
			Versioned: v(), TenantID: "acme", AgentName: "a", AgentVersion: 1,
			Input: json.RawMessage(`{}`),
		}).
		add(journal.TypeRunStarted, journal.RunStartedPayload{Versioned: v(), WorkerID: "w"}).
		add(journal.TypeStepScheduled, journal.StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: journal.StepKindModel, Name: "m",
			IdempotencyKey: "k0", RandSeed: 7,
		}).
		add(journal.TypeModelCallStarted, journal.ModelCallStartedPayload{
			Versioned: v(), StepIndex: 0, Model: "m",
		}).
		add(journal.TypeModelCallCompleted, journal.ModelCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Model: "m",
			Response: json.RawMessage(`{"text":"hello"}`), PromptTokens: 10, CompletionTokens: 5,
		}).
		add(journal.TypeStepScheduled, journal.StepScheduledPayload{
			Versioned: v(), StepIndex: 1, Kind: journal.StepKindTool, Name: "echo",
			IdempotencyKey: "k1",
		}).
		add(journal.TypeToolCallStarted, journal.ToolCallStartedPayload{
			Versioned: v(), StepIndex: 1, Tool: "echo",
		}).
		add(journal.TypeToolCallCompleted, journal.ToolCallCompletedPayload{
			Versioned: v(), StepIndex: 1, Output: json.RawMessage(`{"ok":true}`),
		}).
		add(journal.TypeRunCompleted, journal.RunCompletedPayload{
			Versioned: v(), Output: json.RawMessage(`{"answer":"hello"}`),
		})
}

func build(t *testing.T, l *log) *replay.Recording {
	t.Helper()
	rec, err := replay.Build(l.events)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return rec
}

// ---------------------------------------------------------------------------

// An identical sequence matches, even though every timestamp differs. That is
// the point: a replay happens later, so comparing times would make every replay
// diverge at the first event and the check would be worthless.
func TestCompareMatchesDespiteDifferentTimestamps(t *testing.T) {
	original := build(t, completeRun(t, runA, base))
	// Replayed a week later.
	replayed := completeRun(t, uuid.New(), base.Add(7*24*time.Hour))

	if d := replay.Compare(original, replayed.events); d != nil {
		t.Errorf("identical sequences reported a divergence: %v", d)
	}
}

// The most serious kind: the graph took a different path.
func TestCompareDetectsADifferentEventType(t *testing.T) {
	original := build(t, completeRun(t, runA, base))

	// The replay's model call failed where the original's succeeded.
	diverged := newLog(t, uuid.New(), base).
		add(journal.TypeRunSubmitted, journal.RunSubmittedPayload{
			Versioned: v(), TenantID: "acme", AgentName: "a", AgentVersion: 1,
			Input: json.RawMessage(`{}`),
		}).
		add(journal.TypeRunStarted, journal.RunStartedPayload{Versioned: v(), WorkerID: "w"}).
		add(journal.TypeStepScheduled, journal.StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: journal.StepKindModel, Name: "m", IdempotencyKey: "k0",
		}).
		add(journal.TypeModelCallStarted, journal.ModelCallStartedPayload{
			Versioned: v(), StepIndex: 0, Model: "m",
		}).
		add(journal.TypeStepFailed, journal.StepFailedPayload{
			Versioned: v(), StepIndex: 0, Error: "boom", Retryable: true,
		})

	d := replay.Compare(original, diverged.events)
	if d == nil {
		t.Fatal("a different event type was not detected")
	}
	if d.Kind != replay.DivergenceEventSequence {
		t.Errorf("kind = %q, want event_sequence", d.Kind)
	}
	// It must report the FIRST difference, at seq 5, not a later consequence.
	if d.Seq != 5 {
		t.Errorf("Seq = %d, want 5 (the first point of difference)", d.Seq)
	}
}

// Same events, different step: a step index mismatch means the walk is off.
func TestCompareDetectsADifferentStepIndex(t *testing.T) {
	original := build(t, completeRun(t, runA, base))

	diverged := completeRun(t, uuid.New(), base)
	// Rewrite the tool call to claim a different step.
	raw, err := journal.MarshalPayload(journal.ToolCallStartedPayload{
		Versioned: v(), StepIndex: 5, Tool: "echo",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	diverged.events[6].Payload = raw

	d := replay.Compare(original, diverged.events)
	if d == nil {
		t.Fatal("a step index mismatch was not detected")
	}
	if d.Kind != replay.DivergenceEventSequence {
		t.Errorf("kind = %q", d.Kind)
	}
}

// A replay that stops early, or runs on, has done different work.
func TestCompareDetectsADifferentLength(t *testing.T) {
	original := build(t, completeRun(t, runA, base))

	short := completeRun(t, uuid.New(), base)
	short.events = short.events[:len(short.events)-1] // never finished

	d := replay.Compare(original, short.events)
	if d == nil {
		t.Fatal("a truncated replay was not detected")
	}
	if d.Kind != replay.DivergenceLength {
		t.Errorf("kind = %q, want length", d.Kind)
	}
}

// Identical events, different answer. Rare and serious: it means something
// outside the journal reached the result.
func TestCompareFinalDetectsADifferentTerminalOutput(t *testing.T) {
	original := build(t, completeRun(t, runA, base))

	replayed := completeRun(t, uuid.New(), base)
	raw, err := journal.MarshalPayload(journal.RunCompletedPayload{
		Versioned: v(), Output: json.RawMessage(`{"answer":"something else"}`),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	replayed.events[len(replayed.events)-1].Payload = raw

	// The event SEQUENCE still matches -- same types, same order.
	if d := replay.Compare(original, replayed.events); d != nil {
		t.Fatalf("the sequences should still match: %v", d)
	}

	st, err := journal.Fold(replayed.events)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	d := replay.CompareFinal(original, st)
	if d == nil {
		t.Fatal("a different terminal output was not detected")
	}
	if d.Kind != replay.DivergenceTerminalOutput {
		t.Errorf("kind = %q, want terminal_output", d.Kind)
	}
}

// jsonb normalises key order, so two semantically identical outputs can differ
// byte-for-byte after a database round trip. Comparing bytes would report
// divergences that are purely an artifact of storage.
func TestCompareFinalIgnoresKeyOrder(t *testing.T) {
	original := build(t, completeRun(t, runA, base))
	original.TerminalOutput = json.RawMessage(`{"b":2,"a":1}`)

	st := &journal.RunState{
		Status:         journal.RunStatusCompleted,
		TerminalOutput: json.RawMessage(`{"a":1,"b":2}`),
	}
	if d := replay.CompareFinal(original, st); d != nil {
		t.Errorf("key order was treated as a divergence: %v", d)
	}
}

func TestCompareFinalDetectsADifferentStatus(t *testing.T) {
	original := build(t, completeRun(t, runA, base))
	st := &journal.RunState{Status: journal.RunStatusFailed, TerminalOutput: original.TerminalOutput}

	d := replay.CompareFinal(original, st)
	if d == nil {
		t.Fatal("a different terminal status was not detected")
	}
	if d.Kind != replay.DivergenceStatus {
		t.Errorf("kind = %q, want status", d.Kind)
	}
}

// ---------------------------------------------------------------------------
// Recording
// ---------------------------------------------------------------------------

// A step that failed once and then succeeded must record BOTH attempts.
// Returning the success immediately would make the replay skip StepFailed and
// RetryScheduled -- a shorter sequence, and a divergence for a reason that is
// not a real bug. Every run that ever retried would score as a failure.
func TestRecordingKeepsEveryAttempt(t *testing.T) {
	l := newLog(t, runA, base).
		add(journal.TypeRunSubmitted, journal.RunSubmittedPayload{
			Versioned: v(), TenantID: "acme", AgentName: "a", AgentVersion: 1,
			Input: json.RawMessage(`{}`),
		}).
		add(journal.TypeRunStarted, journal.RunStartedPayload{Versioned: v(), WorkerID: "w"}).
		add(journal.TypeStepScheduled, journal.StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: journal.StepKindModel, Name: "m", IdempotencyKey: "k0",
		}).
		add(journal.TypeModelCallStarted, journal.ModelCallStartedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 0, Model: "m",
		}).
		add(journal.TypeStepFailed, journal.StepFailedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 0, Error: "503", Retryable: true,
		}).
		add(journal.TypeRetryScheduled, journal.RetryScheduledPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, DelayMS: 200,
		}).
		add(journal.TypeModelCallStarted, journal.ModelCallStartedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, Model: "m",
		}).
		add(journal.TypeModelCallCompleted, journal.ModelCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, Model: "m",
			Response: json.RawMessage(`{"text":"ok"}`),
		})

	rec := build(t, l)
	step := rec.Steps[0]
	if len(step.Attempts) != 2 {
		t.Fatalf("recorded %d attempt(s), want 2", len(step.Attempts))
	}
	if !step.Attempts[0].Failed {
		t.Error("attempt 0 is not recorded as failed; the replay would skip the retry")
	}
	if !step.Attempts[0].Retryable {
		t.Error("attempt 0 lost its retryable flag; the replay would fail terminally instead of retrying")
	}
	if step.Attempts[1].Failed {
		t.Error("attempt 1 is recorded as failed")
	}
	if string(step.Attempts[1].ModelResponse) != `{"text":"ok"}` {
		t.Errorf("attempt 1 response = %s", step.Attempts[1].ModelResponse)
	}
}

// The signature is the SHAPE of an event, not its bytes. Timestamps and ids
// must be excluded or every replay diverges at event one.
func TestSignatureExcludesTimeAndIdentity(t *testing.T) {
	payload, err := journal.MarshalPayload(journal.ModelCallStartedPayload{
		Versioned: v(), StepIndex: 3, Attempt: 2, Model: "m",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	a := journal.Event{ID: 1, RunID: runA, Seq: 9, Type: journal.TypeModelCallStarted,
		Payload: payload, CreatedAt: base}
	b := journal.Event{ID: 9999, RunID: uuid.New(), Seq: 9, Type: journal.TypeModelCallStarted,
		Payload: payload, CreatedAt: base.Add(99 * time.Hour)}

	sa, err := replay.SignatureOf(a)
	if err != nil {
		t.Fatalf("SignatureOf: %v", err)
	}
	sb, err := replay.SignatureOf(b)
	if err != nil {
		t.Fatalf("SignatureOf: %v", err)
	}
	if sa != sb {
		t.Errorf("signatures differ on id/time/run alone:\n a: %+v\n b: %+v", sa, sb)
	}
	if sa.StepIndex != 3 {
		t.Errorf("step index not captured: %+v", sa)
	}
	if sa.Attempt != 2 {
		t.Errorf("attempt not captured: %+v", sa)
	}
}

// ---------------------------------------------------------------------------
// Fidelity report
// ---------------------------------------------------------------------------

func TestFidelityExcludesErroredRunsFromTheDenominator(t *testing.T) {
	r := &replay.Report{}
	r.Add(replay.Result{Matched: true})
	r.Add(replay.Result{Matched: true})
	r.Add(replay.Result{Matched: true})
	r.Add(replay.Result{Divergence: &replay.Divergence{Kind: replay.DivergenceEventSequence}})
	// A replay that could not run says nothing about determinism.
	r.Add(replay.Result{Err: "worker never picked it up"})

	if got := r.Fidelity(); got != 75 {
		t.Errorf("Fidelity = %.1f, want 75 (3 of 4 comparable, the errored one excluded)", got)
	}
	if r.Total != 5 || r.Errored != 1 {
		t.Errorf("Total=%d Errored=%d, want 5/1", r.Total, r.Errored)
	}
}

func TestFidelityReportsZeroWithNothingComparable(t *testing.T) {
	r := &replay.Report{}
	r.Add(replay.Result{Err: "nope"})
	if got := r.Fidelity(); got != 0 {
		t.Errorf("Fidelity = %.1f, want 0", got)
	}
}

// Ten failures of one kind is one bug; ten of ten kinds is a broken design.
func TestReportGroupsDivergencesByCause(t *testing.T) {
	r := &replay.Report{}
	r.Add(replay.Result{Divergence: &replay.Divergence{Kind: replay.DivergenceEventSequence}})
	r.Add(replay.Result{Divergence: &replay.Divergence{Kind: replay.DivergenceEventSequence}})
	r.Add(replay.Result{Divergence: &replay.Divergence{Kind: replay.DivergenceTerminalOutput}})

	kinds := r.ByKind()
	if kinds[replay.DivergenceEventSequence] != 2 {
		t.Errorf("event_sequence = %d, want 2", kinds[replay.DivergenceEventSequence])
	}
	if kinds[replay.DivergenceTerminalOutput] != 1 {
		t.Errorf("terminal_output = %d, want 1", kinds[replay.DivergenceTerminalOutput])
	}
}
