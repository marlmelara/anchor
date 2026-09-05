package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/idem"
	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/store"
)

// These tests run against a real Postgres. There is no mock: every property
// they check -- unique constraints, trigger enforcement, transaction rollback --
// is a property of the database, and a fake would only test the fake.
//
//	docker compose up -d
//	ANCHOR_TEST_DATABASE_URL=postgres://anchor:anchor@localhost:5433/anchor?sslmode=disable go test ./...
func testStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("ANCHOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ANCHOR_TEST_DATABASE_URL is not set; skipping database tests")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 8})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)

	if _, err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// TRUNCATE rather than DELETE: the append-only trigger on run_events is a
	// row-level trigger, and TRUNCATE does not fire row-level triggers.
	_, err = s.Pool().Exec(ctx,
		`TRUNCATE queue, leases, steps, run_events, runs, agents RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

// seedAgent registers a trivial agent so runs have a valid (name, version) to
// point at.
func seedAgent(t *testing.T, s *store.Store, name string) int {
	t.Helper()
	def := json.RawMessage(`{"agent":"` + name + `","v":1,"nodes":[]}`)
	version, err := s.RegisterAgent(context.Background(), name, def)
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}
	return version
}

func submit(t *testing.T, s *store.Store, agent string, version int) *journal.RunState {
	t.Helper()
	st, err := s.SubmitRun(context.Background(), store.SubmitRequest{
		TenantID:     "acme",
		AgentName:    agent,
		AgentVersion: version,
		Input:        json.RawMessage(`{"q":"hello"}`),
		BudgetTokens: 10000,
		BudgetCents:  100,
	})
	if err != nil {
		t.Fatalf("submit run: %v", err)
	}
	return st
}

func v() journal.Versioned { return journal.Versioned{V: journal.CurrentPayloadVersion} }

// stepKey derives a step's idempotency key the same way the worker will. The
// index is globally unique, so tests must not hand-write keys or two runs doing
// the same work will collide -- which is exactly what the index is for.
func stepKey(t *testing.T, runID uuid.UUID, stepIndex int, input string) string {
	t.Helper()
	k, err := idem.Key(runID, stepIndex, json.RawMessage(input))
	if err != nil {
		t.Fatalf("idem.Key: %v", err)
	}
	return k
}

// ---------------------------------------------------------------------------

func TestSubmitRunWritesEventAndEnqueuesAtomically(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)

	if st.Status != journal.RunStatusPending {
		t.Errorf("status = %q, want pending", st.Status)
	}
	if st.NextSeq != 1 {
		t.Errorf("NextSeq = %d, want 1", st.NextSeq)
	}

	// The run is in the log...
	events, err := s.LoadEvents(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	if len(events) != 1 || events[0].Type != journal.TypeRunSubmitted || events[0].Seq != 0 {
		t.Fatalf("unexpected log: %+v", events)
	}

	// ...and in the queue. A run in one but not the other is either never
	// executed or claimed with nothing to fold.
	var queued int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM queue WHERE run_id = $1`, st.RunID).Scan(&queued); err != nil {
		t.Fatalf("count queue: %v", err)
	}
	if queued != 1 {
		t.Errorf("queue rows = %d, want 1", queued)
	}
}

func TestLoadStateMatchesInMemoryState(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)

	err := s.Append(ctx, st,
		store.PendingEvent{Type: journal.TypeRunStarted,
			Payload: journal.RunStartedPayload{Versioned: v(), WorkerID: "w-1"}},
		store.PendingEvent{Type: journal.TypeStepScheduled,
			Payload: journal.StepScheduledPayload{
				Versioned: v(), StepIndex: 0, Kind: journal.StepKindModel,
				Name: "answer", NodeID: "n1", IdempotencyKey: stepKey(t, st.RunID, 0, `{"q":"hello"}`), RandSeed: 5,
			}},
		store.PendingEvent{Type: journal.TypeModelCallStarted,
			Payload: journal.ModelCallStartedPayload{
				Versioned: v(), StepIndex: 0, Model: "mock",
				Request: json.RawMessage(`{"messages":[]}`),
			}},
		store.PendingEvent{Type: journal.TypeModelCallCompleted,
			Payload: journal.ModelCallCompletedPayload{
				Versioned: v(), StepIndex: 0, Model: "mock",
				Response:     json.RawMessage(`{"text":"hi"}`),
				PromptTokens: 11, CompletionTokens: 7, Cents: 2,
			}},
		store.PendingEvent{Type: journal.TypeRunCompleted,
			Payload: journal.RunCompletedPayload{Versioned: v(), Output: json.RawMessage(`{"ok":true}`)}},
	)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	loaded, err := s.LoadState(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}

	inMem, _ := json.Marshal(st)
	fromLog, _ := json.Marshal(loaded)
	if string(inMem) != string(fromLog) {
		t.Errorf("state held by the writer diverged from the state folded from the log:\n writer: %s\n    log: %s", inMem, fromLog)
	}
	if loaded.TokensUsed != 18 || loaded.CentsUsed != 2 {
		t.Errorf("accounting = %d tokens / %d cents, want 18/2", loaded.TokensUsed, loaded.CentsUsed)
	}
}

// The (run_id, seq) unique constraint is Anchor's entire concurrency control.
// Two workers that both believe they own a run compute the same next seq; the
// database picks the winner.
func TestConcurrentAppendLosesOnSeqConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)

	// Two workers, each holding a state folded from the same log.
	workerA, err := s.LoadState(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	workerB, err := s.LoadState(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	if workerA.NextSeq != workerB.NextSeq {
		t.Fatalf("test setup: workers disagree on next seq (%d vs %d)", workerA.NextSeq, workerB.NextSeq)
	}

	start := store.PendingEvent{Type: journal.TypeRunStarted,
		Payload: journal.RunStartedPayload{Versioned: v(), WorkerID: "w-a"}}
	if err := s.Append(ctx, workerA, start); err != nil {
		t.Fatalf("worker A append: %v", err)
	}

	err = s.Append(ctx, workerB, store.PendingEvent{Type: journal.TypeRunStarted,
		Payload: journal.RunStartedPayload{Versioned: v(), WorkerID: "w-b"}})
	if !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("worker B: err = %v, want ErrSeqConflict", err)
	}

	// The loser's state is untouched, so it can safely release and move on.
	if workerB.NextSeq != 1 {
		t.Errorf("loser's NextSeq = %d, want 1 (state must not advance on a failed append)", workerB.NextSeq)
	}

	// Exactly one RunStarted landed.
	events, err := s.LoadEvents(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("log has %d events, want 2", len(events))
	}
	final, err := s.LoadState(ctx, st.RunID)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if final.WorkerID != "w-a" {
		t.Errorf("WorkerID = %q, want w-a", final.WorkerID)
	}
}

// Append-only is enforced by the database, not by the application remembering
// not to write.
func TestRunEventsRejectsUpdateAndDelete(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)

	_, err := s.Pool().Exec(ctx,
		`UPDATE run_events SET type = 'Tampered' WHERE run_id = $1`, st.RunID)
	if err == nil {
		t.Error("UPDATE on run_events succeeded; the log is not append-only")
	}

	_, err = s.Pool().Exec(ctx, `DELETE FROM run_events WHERE run_id = $1`, st.RunID)
	if err == nil {
		t.Error("DELETE on run_events succeeded; the log is not append-only")
	}

	// The log is intact.
	events, err := s.LoadEvents(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	if len(events) != 1 || events[0].Type != journal.TypeRunSubmitted {
		t.Errorf("log was modified: %+v", events)
	}
}

// A rejected event must leave no trace. If a bad event could half-commit, the
// log would no longer be a faithful record.
func TestFailedAppendLeavesNoTrace(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)

	before := st.Clone()

	// step_index 4 is not the next index; the fold rejects it before any SQL.
	err := s.Append(ctx, st,
		store.PendingEvent{Type: journal.TypeRunStarted,
			Payload: journal.RunStartedPayload{Versioned: v(), WorkerID: "w-1"}},
		store.PendingEvent{Type: journal.TypeStepScheduled,
			Payload: journal.StepScheduledPayload{
				Versioned: v(), StepIndex: 4, Kind: journal.StepKindModel, IdempotencyKey: "k",
			}},
	)
	if err == nil {
		t.Fatal("append accepted an out-of-order step index")
	}

	if st.NextSeq != before.NextSeq || st.Status != before.Status {
		t.Errorf("caller state mutated by a failed append: %+v", st)
	}
	events, err := s.LoadEvents(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("log has %d events, want 1; the valid half of a rejected batch was committed", len(events))
	}
}

// ---------------------------------------------------------------------------
// rebuild: the proof that runs and steps are projections, not state
// ---------------------------------------------------------------------------

// fullLifecycle drives a run through a retry and a tool call so the projection
// has something non-trivial to reproduce.
func fullLifecycle(t *testing.T, s *store.Store, st *journal.RunState) {
	t.Helper()
	ctx := context.Background()
	steps := []store.PendingEvent{
		{Type: journal.TypeRunStarted, Payload: journal.RunStartedPayload{Versioned: v(), WorkerID: "w-1"}},
		{Type: journal.TypeStepScheduled, Payload: journal.StepScheduledPayload{
			Versioned: v(), StepIndex: 0, Kind: journal.StepKindModel,
			Name: "plan", NodeID: "n1", IdempotencyKey: stepKey(t, st.RunID, 0, `{"q":"hello"}`), RandSeed: 11}},
		{Type: journal.TypeModelCallStarted, Payload: journal.ModelCallStartedPayload{
			Versioned: v(), StepIndex: 0, Model: "mock"}},
		{Type: journal.TypeStepFailed, Payload: journal.StepFailedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 0, Error: "mock 503", Retryable: true}},
		{Type: journal.TypeRetryScheduled, Payload: journal.RetryScheduledPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, DelayMS: 100}},
		{Type: journal.TypeModelCallStarted, Payload: journal.ModelCallStartedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, Model: "mock"}},
		{Type: journal.TypeModelCallCompleted, Payload: journal.ModelCallCompletedPayload{
			Versioned: v(), StepIndex: 0, Attempt: 1, Model: "mock",
			Response:     json.RawMessage(`{"tool":"web_fetch"}`),
			PromptTokens: 40, CompletionTokens: 20, Cents: 4}},
		{Type: journal.TypeStepScheduled, Payload: journal.StepScheduledPayload{
			Versioned: v(), StepIndex: 1, Kind: journal.StepKindTool,
			Name: "web_fetch", NodeID: "n1", IdempotencyKey: stepKey(t, st.RunID, 1, `{"url":"https://example.com"}`), RandSeed: 12}},
		{Type: journal.TypeToolCallStarted, Payload: journal.ToolCallStartedPayload{
			Versioned: v(), StepIndex: 1, Tool: "web_fetch"}},
		{Type: journal.TypeToolCallCompleted, Payload: journal.ToolCallCompletedPayload{
			Versioned: v(), StepIndex: 1, ExitCode: 0, Output: json.RawMessage(`{"body":"ok"}`)}},
		{Type: journal.TypeRunCompleted, Payload: journal.RunCompletedPayload{
			Versioned: v(), Output: json.RawMessage(`{"answer":"ok"}`)}},
	}
	for _, e := range steps {
		if err := s.Append(ctx, st, e); err != nil {
			t.Fatalf("append %s: %v", e.Type, err)
		}
	}
}

func TestRebuildRecomputesProjectionsFromLog(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)
	fullLifecycle(t, s, st)

	// Corrupt the projection the way a bug in the append path would.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE runs SET status = 'running', tokens_used = 0, cents_used = 0 WHERE id = $1`,
		st.RunID); err != nil {
		t.Fatalf("corrupt runs: %v", err)
	}
	if _, err := s.Pool().Exec(ctx,
		`UPDATE steps SET status = 'scheduled', attempt = 0, tokens = 0 WHERE run_id = $1`,
		st.RunID); err != nil {
		t.Fatalf("corrupt steps: %v", err)
	}

	// Verify sees the damage.
	res, err := s.RebuildRun(ctx, st.RunID, true)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(res.Mismatch) == 0 {
		t.Fatal("verify reported no mismatch against a deliberately corrupted projection")
	}

	// Rebuild repairs it from run_events alone.
	if _, err := s.RebuildRun(ctx, st.RunID, false); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	res, err = s.RebuildRun(ctx, st.RunID, true)
	if err != nil {
		t.Fatalf("verify after rebuild: %v", err)
	}
	if len(res.Mismatch) != 0 {
		t.Errorf("projection still differs after rebuild: %v", res.Mismatch)
	}

	// And the rebuilt projection agrees with the fold on the details.
	var status string
	var tokens, cents int64
	if err := s.Pool().QueryRow(ctx,
		`SELECT status, tokens_used, cents_used FROM runs WHERE id = $1`, st.RunID,
	).Scan(&status, &tokens, &cents); err != nil {
		t.Fatalf("read rebuilt run: %v", err)
	}
	if status != "completed" || tokens != 60 || cents != 4 {
		t.Errorf("rebuilt run = %s / %d tokens / %d cents, want completed/60/4", status, tokens, cents)
	}

	var attempt int
	if err := s.Pool().QueryRow(ctx,
		`SELECT attempt FROM steps WHERE run_id = $1 AND step_index = 0`, st.RunID,
	).Scan(&attempt); err != nil {
		t.Fatalf("read rebuilt step: %v", err)
	}
	if attempt != 1 {
		t.Errorf("rebuilt step attempt = %d, want 1", attempt)
	}
}

// The live append path and the rebuild must agree with no repair needed. This
// is the invariant that keeps "run_events is the source of truth" true rather
// than aspirational.
func TestLiveProjectionAlreadyMatchesRebuild(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := seedAgent(t, s, "research")
	for range 3 {
		st := submit(t, s, "research", version)
		fullLifecycle(t, s, st)
	}

	res, err := s.RebuildAll(ctx, true)
	if err != nil {
		t.Fatalf("verify all: %v", err)
	}
	if res.Runs != 3 {
		t.Errorf("verified %d runs, want 3", res.Runs)
	}
	if len(res.Mismatch) != 0 {
		t.Errorf("the append path and the fold disagree: %v", res.Mismatch)
	}
}

// ---------------------------------------------------------------------------

func TestRegisterAgentIncrementsVersion(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	v1, err := s.RegisterAgent(ctx, "research", json.RawMessage(`{"nodes":[]}`))
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	v2, err := s.RegisterAgent(ctx, "research", json.RawMessage(`{"nodes":[{"id":"n1"}]}`))
	if err != nil {
		t.Fatalf("register v2: %v", err)
	}
	if v1 != 1 || v2 != 2 {
		t.Fatalf("versions = %d, %d; want 1, 2", v1, v2)
	}

	// The old definition is still readable, which is what lets a replay walk
	// the graph the original run actually ran.
	def, err := s.GetAgent(ctx, "research", 1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	// definition is a jsonb column, so it comes back semantically equal but
	// re-formatted. Compare meaning, not bytes.
	var got, want any
	if err := json.Unmarshal(def, &got); err != nil {
		t.Fatalf("unmarshal stored definition: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"nodes":[]}`), &want); err != nil {
		t.Fatalf("unmarshal expected definition: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("v1 definition = %s, want the original graph", def)
	}

	latest, err := s.LatestAgentVersion(ctx, "research")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest != 2 {
		t.Errorf("latest = %d, want 2", latest)
	}
}

func TestLoadStateOnUnknownRun(t *testing.T) {
	s := testStore(t)
	_, err := s.LoadState(context.Background(), uuid.New())
	if !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestStoreClockIsInjectable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	pinned := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return pinned }

	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)

	loaded, err := s.LoadState(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !loaded.CreatedAt.Equal(pinned) {
		t.Errorf("CreatedAt = %v, want the injected clock %v", loaded.CreatedAt, pinned)
	}
	// Recorded time is what the run observes, never the wall clock.
	if !loaded.Now().Equal(pinned) {
		t.Errorf("Now() = %v, want %v", loaded.Now(), pinned)
	}
}

// Postgres timestamptz keeps microseconds; Go's time.Now() carries nanoseconds.
// If the writer stamps an event at nanosecond precision, the state it holds in
// memory stops being equal to the same run folded back out of the log, and the
// fold is no longer the single source of truth.
//
// The clock here is pinned to a time with non-zero nanoseconds so this fails
// regardless of the host's clock granularity -- the original bug was invisible
// on macOS and only appeared on Linux.
func TestEventTimestampsSurviveTheRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	nanos := time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.UTC)
	s.Now = func() time.Time { return nanos }

	version := seedAgent(t, s, "research")
	st := submit(t, s, "research", version)
	if err := s.Append(ctx, st, store.PendingEvent{
		Type:    journal.TypeRunStarted,
		Payload: journal.RunStartedPayload{Versioned: v(), WorkerID: "w-1"},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	loaded, err := s.LoadState(ctx, st.RunID)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}

	if !st.CreatedAt.Equal(loaded.CreatedAt) {
		t.Errorf("CreatedAt does not survive the round trip:\n writer: %s\n    log: %s",
			st.CreatedAt.Format(time.RFC3339Nano), loaded.CreatedAt.Format(time.RFC3339Nano))
	}

	// The stamped value must already be at storable precision, not merely
	// compare equal after the database rounds it.
	want := nanos.Truncate(time.Microsecond)
	if !st.CreatedAt.Equal(want) {
		t.Errorf("writer stamped %s, want it truncated to %s",
			st.CreatedAt.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
	if st.CreatedAt.Nanosecond()%1000 != 0 {
		t.Errorf("stamped time has sub-microsecond precision Postgres cannot store: %s",
			st.CreatedAt.Format(time.RFC3339Nano))
	}

	// Whole-state equality is the property that actually matters.
	inMem, _ := json.Marshal(st)
	fromLog, _ := json.Marshal(loaded)
	if string(inMem) != string(fromLog) {
		t.Errorf("writer state diverged from the folded log:\n writer: %s\n    log: %s", inMem, fromLog)
	}
}
