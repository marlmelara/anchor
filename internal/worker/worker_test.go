package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/idem"
	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/provider"
	"github.com/marlmelara/anchor/internal/store"
	"github.com/marlmelara/anchor/internal/testsupport"
	"github.com/marlmelara/anchor/internal/tool"
	"github.com/marlmelara/anchor/internal/worker"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	return testsupport.Store(t, "test_worker")
}

// twoNodeGraph is the M2 target: one model_call feeding one tool_call.
const twoNodeGraph = `{
  "agent": "two-step",
  "v": 1,
  "nodes": [
    {"id": "n1", "type": "model_call", "model": "mock", "prompt": "Answer: {{input.topic}}"},
    {"id": "n2", "type": "tool_call", "tool": "echo", "input": {"said": "{{n1.output}}"}}
  ]
}`

type harness struct {
	store    *store.Store
	provider *provider.Mock
	registry *tool.Registry
	runner   *tool.MockRunner
	counter  *tool.Counter
}

func newHarness(t *testing.T, s *store.Store) *harness {
	t.Helper()
	h := &harness{
		store:    s,
		provider: provider.NewMock("done"),
		registry: tool.NewRegistry(),
		runner:   tool.NewMockRunner(),
		counter:  &tool.Counter{},
	}
	// No artificial latency in tests: these assert behaviour, not throughput.
	h.provider.Latency = 0

	echoDef, echoFn := tool.Echo("echo")
	if err := h.registry.Register(echoDef); err != nil {
		t.Fatalf("register echo: %v", err)
	}
	h.runner.Handle("echo", echoFn)

	countDef := h.counter.Definition("count")
	if err := h.registry.Register(countDef); err != nil {
		t.Fatalf("register count: %v", err)
	}
	h.runner.Handle("count", h.counter.Func())
	return h
}

func (h *harness) newWorker(t *testing.T, id string, pool int) *worker.Worker {
	t.Helper()
	return worker.New(h.store, h.provider, h.registry, h.runner, worker.Config{
		WorkerID:       id,
		PoolSize:       pool,
		LeaseFor:       10 * time.Second,
		HeartbeatEvery: time.Second,
		IdlePoll:       5 * time.Millisecond,
		BaseBackoff:    time.Millisecond,
		Logf:           func(format string, args ...any) { t.Logf(format, args...) },
	})
}

func registerAgent(t *testing.T, s *store.Store, name, graph string) int {
	t.Helper()
	version, err := s.RegisterAgent(context.Background(), name, json.RawMessage(graph))
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}
	return version
}

func submit(t *testing.T, s *store.Store, name string, version int, input string) uuid.UUID {
	t.Helper()
	st, err := s.SubmitRun(context.Background(), store.SubmitRequest{
		TenantID: "acme", AgentName: name, AgentVersion: version,
		Input: json.RawMessage(input), BudgetTokens: 100000, BudgetMicros: 1000,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return st.RunID
}

// runUntilDrained runs workers until the queue is empty and nothing is leased,
// or the deadline passes.
func runUntilDrained(t *testing.T, h *harness, workers []*worker.Worker, timeout time.Duration) {
	t.Helper()
	claimCtx, stopClaiming := context.WithCancel(context.Background())
	runCtx, stopRuns := context.WithCancel(context.Background())
	defer stopRuns()

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(w *worker.Worker) {
			defer wg.Done()
			if err := w.Run(claimCtx, runCtx); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("worker %s: %v", w.ID(), err)
			}
		}(w)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var pending int64
		if err := h.store.Pool().QueryRow(context.Background(),
			`SELECT (SELECT count(*) FROM queue) + (SELECT count(*) FROM leases)`).Scan(&pending); err != nil {
			t.Fatalf("count pending: %v", err)
		}
		if pending == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopClaiming()
	wg.Wait()
}

func loadState(t *testing.T, s *store.Store, runID uuid.UUID) *journal.RunState {
	t.Helper()
	st, err := s.LoadState(context.Background(), runID)
	if err != nil {
		t.Fatalf("load state %s: %v", runID, err)
	}
	return st
}

// ---------------------------------------------------------------------------

func TestWorkerExecutesTwoNodeGraph(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"durable execution"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	st := loadState(t, s, runID)
	if st.Status != journal.RunStatusCompleted {
		t.Fatalf("status = %q (error: %s), want completed", st.Status, st.Error)
	}
	if len(st.Steps) != 2 {
		t.Fatalf("len(Steps) = %d, want 2 (one model, one tool)", len(st.Steps))
	}
	if st.Steps[0].Kind != journal.StepKindModel || st.Steps[1].Kind != journal.StepKindTool {
		t.Errorf("step kinds = %q, %q; want model, tool", st.Steps[0].Kind, st.Steps[1].Kind)
	}
	for i, step := range st.Steps {
		if step.Status != journal.StepStatusCompleted {
			t.Errorf("step %d status = %q, want completed", i, step.Status)
		}
		if step.IdempotencyKey == "" {
			t.Errorf("step %d has no idempotency key", i)
		}
	}
	if st.TokensUsed == 0 {
		t.Error("TokensUsed = 0; the model call should have been billed")
	}
}

// The M2 acceptance criterion: 100 submitted runs all complete, with no run
// claimed twice.
//
// "Claimed twice" is checked by counting provider calls rather than by watching
// claims: if any run were executed by two workers, the model call would happen
// more than once and the total would exceed one per run. That is the property
// that actually matters -- a duplicated claim nobody can observe is harmless; a
// duplicated effect is not.
func TestHundredRunsCompleteWithNoDuplicateExecution(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)

	const runCount = 100
	ids := make([]uuid.UUID, 0, runCount)
	for i := range runCount {
		ids = append(ids, submit(t, s, "two-step", version, fmt.Sprintf(`{"topic":"run %d"}`, i)))
	}

	workers := []*worker.Worker{
		h.newWorker(t, "w-1", 4),
		h.newWorker(t, "w-2", 4),
		h.newWorker(t, "w-3", 4),
	}
	runUntilDrained(t, h, workers, 60*time.Second)

	completed := 0
	for _, id := range ids {
		st := loadState(t, s, id)
		if st.Status != journal.RunStatusCompleted {
			t.Errorf("run %s: status = %q, error = %q", id, st.Status, st.Error)
			continue
		}
		completed++
		if len(st.Steps) != 2 {
			t.Errorf("run %s: %d steps, want 2", id, len(st.Steps))
		}
	}
	if completed != runCount {
		t.Fatalf("%d of %d runs completed", completed, runCount)
	}

	// Exactly one model call per run. More would mean a run was executed twice.
	if calls := h.provider.Calls(); calls != runCount {
		t.Errorf("provider calls = %d, want exactly %d (one per run); a higher number means a run was executed more than once", calls, runCount)
	}

	// Every idempotency key is globally unique, enforced by the index. A
	// duplicate would have failed the insert long before here.
	var distinctKeys, totalSteps int
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT count(DISTINCT idempotency_key), count(*) FROM steps`).Scan(&distinctKeys, &totalSteps); err != nil {
		t.Fatalf("count steps: %v", err)
	}
	if distinctKeys != totalSteps || totalSteps != runCount*2 {
		t.Errorf("steps = %d with %d distinct keys, want %d of each", totalSteps, distinctKeys, runCount*2)
	}

	// The projections still agree with the log after 100 concurrent runs.
	res, err := s.RebuildAll(context.Background(), true)
	if err != nil {
		t.Fatalf("verify projections: %v", err)
	}
	if len(res.Mismatch) != 0 {
		t.Errorf("projections drifted under concurrency: %v", res.Mismatch)
	}
}

// The second half of the M2 acceptance criterion: running a step twice with the
// same key executes it once.
func TestReserveStepMakesTheSecondAttemptANoop(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"x"}`)

	input := json.RawMessage(`{"url":"https://example.com"}`)

	reserveOnce := func() store.Reservation {
		tx, err := s.Pool().Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		r, err := s.ReserveStep(ctx, tx, runID, 0, journal.StepKindTool, "count", input)
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("reserve: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
		return r
	}

	first := reserveOnce()
	if first.AlreadyRan {
		t.Fatal("first reservation reported AlreadyRan; nothing had run yet")
	}

	second := reserveOnce()
	if !second.AlreadyRan {
		t.Error("second reservation did not report AlreadyRan; the step would execute twice")
	}
	if second.Key != first.Key {
		t.Errorf("keys differ between attempts: %s vs %s", first.Key, second.Key)
	}

	// Simulate what the worker does: only execute when the reservation is new.
	for _, r := range []store.Reservation{first, second} {
		if !r.AlreadyRan {
			if _, err := h.runner.Run(ctx, h.counter.Definition("count"), input); err != nil {
				t.Fatalf("run tool: %v", err)
			}
		}
	}
	if got := h.counter.Count(); got != 1 {
		t.Errorf("effect fired %d times, want exactly 1", got)
	}

	var rows int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM steps WHERE run_id = $1`, runID).Scan(&rows); err != nil {
		t.Fatalf("count steps: %v", err)
	}
	if rows != 1 {
		t.Errorf("steps rows = %d, want 1", rows)
	}
	_ = h
}

func TestRetriesRecoverFromRetryableProviderErrors(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	// The first two calls fail with a 503; the third succeeds.
	h.provider.FailFirst = 2

	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"flaky"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 20*time.Second)

	st := loadState(t, s, runID)
	if st.Status != journal.RunStatusCompleted {
		t.Fatalf("status = %q (error: %s), want completed after retries", st.Status, st.Error)
	}
	if st.Steps[0].Attempt != 2 {
		t.Errorf("model step attempt = %d, want 2 after two failures", st.Steps[0].Attempt)
	}
	if got := len(st.Steps[0].RetryDelaysMS); got != 2 {
		t.Errorf("recorded %d retry delays, want 2; replay needs them to skip the waits", got)
	}
	// The retried step is one logical step, not three.
	if len(st.Steps) != 2 {
		t.Errorf("len(Steps) = %d, want 2; a retry must not create a new step", len(st.Steps))
	}
}

func TestBudgetExceededStopsTheRunBeforeSpending(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)

	st, err := s.SubmitRun(context.Background(), store.SubmitRequest{
		TenantID: "acme", AgentName: "two-step", AgentVersion: version,
		Input: json.RawMessage(`{"topic":"x"}`),
		// Smaller than one call's projected ceiling, so the first call is
		// refused before any money is spent.
		BudgetTokens: 10,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	final := loadState(t, s, st.RunID)
	if final.Status != journal.RunStatusFailed {
		t.Fatalf("status = %q, want failed", final.Status)
	}
	if h.provider.Calls() != 0 {
		t.Errorf("provider was called %d times; the budget must be enforced before the call", h.provider.Calls())
	}
	if final.TokensUsed != 0 {
		t.Errorf("TokensUsed = %d, want 0", final.TokensUsed)
	}
}

const loopGraph = `{
  "agent": "looper",
  "v": 1,
  "nodes": [
    {"id": "n1", "type": "agent_loop", "model": "mock", "max_iterations": 4, "tools": ["echo"]}
  ]
}`

func TestAgentLoopRunsToolThenFinalAnswer(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	// First call asks for a tool; second gives the final answer.
	h.provider.Script = []provider.Response{
		provider.ToolReply("echo", json.RawMessage(`{"q":"hello"}`), 10, 5),
		provider.TextReply("all done", 12, 6),
	}

	version := registerAgent(t, s, "looper", loopGraph)
	runID := submit(t, s, "looper", version, `{"topic":"loop"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	st := loadState(t, s, runID)
	if st.Status != journal.RunStatusCompleted {
		t.Fatalf("status = %q (error: %s), want completed", st.Status, st.Error)
	}
	// model call, tool call, model call -- each iteration's work is its own
	// step with its own idempotency key.
	if len(st.Steps) != 3 {
		t.Fatalf("len(Steps) = %d, want 3", len(st.Steps))
	}
	kinds := []journal.StepKind{st.Steps[0].Kind, st.Steps[1].Kind, st.Steps[2].Kind}
	want := []journal.StepKind{journal.StepKindModel, journal.StepKindTool, journal.StepKindModel}
	for i := range want {
		if kinds[i] != want[i] {
			t.Errorf("step %d kind = %q, want %q", i, kinds[i], want[i])
		}
	}
}

func TestNonIdempotentToolFailureHaltsForReview(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	// The counting tool is declared non-idempotent; make it fail after its
	// effect, which is exactly the ambiguous window.
	h.runner.Handle("count", func(ctx context.Context, input json.RawMessage) (tool.Result, error) {
		return tool.Result{}, errors.New("process died after the charge was submitted")
	})

	const graph = `{
	  "agent": "risky", "v": 1,
	  "nodes": [{"id": "n1", "type": "tool_call", "tool": "count", "input": {"amount": 100}}]
	}`
	version := registerAgent(t, s, "risky", graph)
	runID := submit(t, s, "risky", version, `{}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	st := loadState(t, s, runID)
	if st.Status != journal.RunStatusNeedsReview {
		t.Fatalf("status = %q, want needs_review; a non-idempotent tool must not be retried blindly", st.Status)
	}
	if st.Steps[0].Status != journal.StepStatusNeedsReview {
		t.Errorf("step status = %q, want needs_review", st.Steps[0].Status)
	}
	if st.Steps[0].Attempt != 0 {
		t.Errorf("attempt = %d, want 0; the step must not have been retried", st.Steps[0].Attempt)
	}
	if st.Error == "" {
		t.Error("no reason recorded; a run halted for a human must say what to check")
	}
}

// Resumption must reuse the completed prefix, not re-walk it at new indexes.
//
// This is a regression test for a real bug: the step index was taken from
// len(st.Steps), which equals the walk position only on a FRESH run. On a
// resumed run the folded state already holds the completed steps, so the walk
// started past the end and executed the whole graph again at higher indexes.
// It passed every single-worker test and only showed up under the chaos loop,
// as runs finishing with 3 or 4 steps instead of 2.
func TestResumeReusesCompletedStepsInsteadOfRepeatingThem(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"resume"}`)

	// Hand-build the journal of a run whose first step finished just before its
	// worker died: started, step 0 scheduled, step 0 completed, nothing else.
	st := loadState(t, s, runID)
	req := json.RawMessage(`{"model":"mock","messages":[]}`)
	key, err := idem.Key(runID, 0, req)
	if err != nil {
		t.Fatalf("idem.Key: %v", err)
	}
	v := journal.Versioned{V: journal.CurrentPayloadVersion}

	// The reservation row the dead worker left behind.
	tx, err := s.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := s.ReserveStep(ctx, tx, runID, 0, journal.StepKindModel, "mock", req); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if err := s.Append(ctx, st,
		store.PendingEvent{Type: journal.TypeRunStarted,
			Payload: journal.RunStartedPayload{Versioned: v, WorkerID: "dead-worker"}},
		store.PendingEvent{Type: journal.TypeStepScheduled,
			Payload: journal.StepScheduledPayload{
				Versioned: v, StepIndex: 0, Kind: journal.StepKindModel, Name: "mock",
				NodeID: "n1", IdempotencyKey: key, RandSeed: 1,
			}},
		store.PendingEvent{Type: journal.TypeModelCallStarted,
			Payload: journal.ModelCallStartedPayload{
				Versioned: v, StepIndex: 0, Model: "mock", Request: req}},
		store.PendingEvent{Type: journal.TypeModelCallCompleted,
			Payload: journal.ModelCallCompletedPayload{
				Versioned: v, StepIndex: 0, Model: "mock",
				Response:     json.RawMessage(`{"text":"already answered"}`),
				PromptTokens: 10, CompletionTokens: 5, Cents: 1}},
	); err != nil {
		t.Fatalf("seed journal: %v", err)
	}

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-2", 1)}, 20*time.Second)

	final := loadState(t, s, runID)
	if final.Status != journal.RunStatusCompleted {
		t.Fatalf("status = %q (error: %s), want completed", final.Status, final.Error)
	}
	if len(final.Steps) != 2 {
		t.Fatalf("len(Steps) = %d, want 2; the resumed walk re-executed the completed prefix", len(final.Steps))
	}
	// The already-finished model call must not have been made again.
	if calls := h.provider.Calls(); calls != 0 {
		t.Errorf("provider called %d times on resume; step 0 was already completed and must be reused", calls)
	}
	if final.Steps[0].IdempotencyKey != key {
		t.Errorf("step 0 key changed on resume: %s", final.Steps[0].IdempotencyKey)
	}
	// Token accounting must not double-count the reused step.
	if final.TokensUsed != 15 {
		t.Errorf("TokensUsed = %d, want 15; a reused step must not be billed twice", final.TokensUsed)
	}
}

// A lease whose owner stopped heartbeating must come back to the queue, or a
// crashed worker's runs are stranded forever.
func TestReaperRequeuesExpiredLeases(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"orphan"}`)

	claimed, err := s.ClaimNext(ctx, "doomed-worker", 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed != runID {
		t.Fatalf("claimed %s, want %s", claimed, runID)
	}

	// Nothing is queued while the run is owned and the lease is live.
	n, err := s.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 0 {
		t.Errorf("reaped %d runs with a live lease, want 0", n)
	}

	// The worker dies: no release, no heartbeat, the lease simply ages out.
	if err := s.ExpireLeaseNow(ctx, runID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	n, err = s.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d runs, want 1", n)
	}
	if _, err := s.Lease(ctx, runID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the expired lease was not removed: %v", err)
	}

	// Another worker can now take it.
	retaken, err := s.ClaimNext(ctx, "rescuer", 30*time.Second)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if retaken != runID {
		t.Errorf("reclaimed %s, want %s", retaken, runID)
	}
	lease, err := s.Lease(ctx, runID)
	if err != nil {
		t.Fatalf("read lease: %v", err)
	}
	if lease.WorkerID != "rescuer" {
		t.Errorf("owner = %q, want rescuer", lease.WorkerID)
	}
}

// A finished run whose worker died before releasing must not be requeued --
// claiming it again would be pure waste.
func TestReaperIgnoresTerminalRuns(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	h := newHarness(t, s)
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"done"}`)

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)
	if st := loadState(t, s, runID); st.Status != journal.RunStatusCompleted {
		t.Fatalf("setup: status = %q", st.Status)
	}

	// Put an expired lease back on the finished run, as a worker that died
	// after RunCompleted but before releasing would leave.
	if _, err := s.Pool().Exec(ctx, `
		INSERT INTO leases (run_id, worker_id, acquired_at, heartbeat_at, expires_at)
		VALUES ($1, 'zombie', now(), now(), now() - interval '1 minute')`, runID); err != nil {
		t.Fatalf("insert stale lease: %v", err)
	}

	n, err := s.ReapExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if n != 0 {
		t.Errorf("reaped %d terminal runs, want 0", n)
	}
	if _, err := s.Lease(ctx, runID); !errors.Is(err, store.ErrNotFound) {
		t.Error("the stale lease should still be cleaned up even when the run is not requeued")
	}
}

// A zombie worker -- one that froze, lost its lease, then woke believing it
// still owned the run -- must not be able to corrupt anything.
func TestZombieWorkerCannotAdvanceARunItLost(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"zombie"}`)

	if _, err := s.ClaimNext(ctx, "zombie", 30*time.Second); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// The state the zombie is holding in memory.
	zombieState := loadState(t, s, runID)

	// It freezes. The lease expires, the reaper requeues, a new worker takes it.
	if err := s.ExpireLeaseNow(ctx, runID); err != nil {
		t.Fatalf("expire: %v", err)
	}
	if _, err := s.ReapExpiredLeases(ctx); err != nil {
		t.Fatalf("reap: %v", err)
	}
	if _, err := s.ClaimNext(ctx, "rescuer", 30*time.Second); err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	rescuerState := loadState(t, s, runID)
	v := journal.Versioned{V: journal.CurrentPayloadVersion}
	if err := s.Append(ctx, rescuerState, store.PendingEvent{
		Type:    journal.TypeRunStarted,
		Payload: journal.RunStartedPayload{Versioned: v, WorkerID: "rescuer", Resumed: true},
	}); err != nil {
		t.Fatalf("rescuer append: %v", err)
	}

	// Now the zombie wakes up and tries to continue from its stale state.
	err := s.Append(ctx, zombieState, store.PendingEvent{
		Type:    journal.TypeRunStarted,
		Payload: journal.RunStartedPayload{Versioned: v, WorkerID: "zombie"},
	})
	if !errors.Is(err, store.ErrSeqConflict) {
		t.Fatalf("zombie append: err = %v, want ErrSeqConflict", err)
	}

	// Its heartbeat also tells it the truth.
	stillOwner, err := s.Heartbeat(ctx, runID, "zombie", 30*time.Second)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if stillOwner {
		t.Error("the zombie's heartbeat reported it still owns the run")
	}

	// And the log is unharmed: one rescuer start, no zombie start after it.
	final := loadState(t, s, runID)
	if final.WorkerID != "rescuer" {
		t.Errorf("owner in the journal = %q, want rescuer", final.WorkerID)
	}
}

// ---------------------------------------------------------------------------
// M5: spending budget
// ---------------------------------------------------------------------------

// pricedGraph uses a REAL model id so the price table applies. The mock
// provider answers it; "mock" itself is priced at zero so benchmarks measure
// Anchor rather than spend.
const pricedGraph = `{
  "agent": "priced",
  "v": 1,
  "nodes": [
    {"id": "n1", "type": "model_call", "model": "claude-opus-5-5", "prompt": "answer {{input.topic}}"}
  ]
}`

// The M5 acceptance criterion: a run over budget stops with BudgetExceeded.
//
// This is the test that could not exist before, because the cost was always
// zero: the mock reported a cost the real adapter never did, so nothing
// counted against a spending cap.
func TestSpendingBudgetStopsTheRunBeforeTheCall(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "priced", pricedGraph)

	st, err := s.SubmitRun(context.Background(), store.SubmitRequest{
		TenantID: "acme", AgentName: "priced", AgentVersion: version,
		Input: json.RawMessage(`{"topic":"x"}`),
		// 100 micro-dollars is $0.0001. One call's projected cost on Opus 5.5
		// at the 1024-token output ceiling is about 20,000 micro-dollars, so
		// this is refused before any money is spent.
		BudgetMicros: 100,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	final := loadState(t, s, st.RunID)
	if final.Status != journal.RunStatusFailed {
		t.Fatalf("status = %q, want failed", final.Status)
	}
	if h.provider.Calls() != 0 {
		t.Errorf("the provider was called %d times; a spending budget must be enforced BEFORE the call",
			h.provider.Calls())
	}
	if final.MicrosUsed != 0 {
		t.Errorf("MicrosUsed = %d, want 0", final.MicrosUsed)
	}
	if !strings.Contains(final.Error, "spending budget") {
		t.Errorf("error = %q, want it to name the spending budget", final.Error)
	}

	// The breach detail must be in the log, not only in the error string.
	events, err := s.LoadEvents(context.Background(), st.RunID)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}
	var found bool
	for _, e := range events {
		if e.Type == journal.TypeBudgetExceeded {
			found = true
			if !strings.Contains(string(e.Payload), `"micros"`) {
				t.Errorf("BudgetExceeded resource is not micros: %s", e.Payload)
			}
		}
	}
	if !found {
		t.Error("no BudgetExceeded event was journaled")
	}
}

// A run within its budget must record what it actually spent, so the next
// call's check has a real figure to add to.
func TestCompletedRunRecordsRealSpend(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	version := registerAgent(t, s, "priced", pricedGraph)

	st, err := s.SubmitRun(context.Background(), store.SubmitRequest{
		TenantID: "acme", AgentName: "priced", AgentVersion: version,
		Input: json.RawMessage(`{"topic":"x"}`), BudgetMicros: 10_000_000,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	final := loadState(t, s, st.RunID)
	if final.Status != journal.RunStatusCompleted {
		t.Fatalf("status = %q (error %s), want completed", final.Status, final.Error)
	}
	// The mock reports 10 prompt and 5 completion tokens. On Opus 5.5 that is
	// 10*$4/1M + 5*$20/1M = 40 + 100 = 140 micro-dollars.
	if final.MicrosUsed != 140 {
		t.Errorf("MicrosUsed = %d, want 140", final.MicrosUsed)
	}
	if final.CostIncomplete {
		t.Error("CostIncomplete = true for a priced model")
	}
}

// An unpriced model with a spending budget set must refuse, not proceed with an
// uncountable cost. Proceeding would leave the operator believing a cap is in
// force while nothing counts against it.
func TestUnpricedModelRefusesWhenASpendingBudgetIsSet(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	const graph = `{"agent":"mystery","v":1,"nodes":[
	  {"id":"n1","type":"model_call","model":"claude-not-a-real-model","prompt":"x"}]}`
	version := registerAgent(t, s, "mystery", graph)

	st, err := s.SubmitRun(context.Background(), store.SubmitRequest{
		TenantID: "acme", AgentName: "mystery", AgentVersion: version,
		Input: json.RawMessage(`{}`), BudgetMicros: 1_000_000,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	final := loadState(t, s, st.RunID)
	if final.Status != journal.RunStatusFailed {
		t.Fatalf("status = %q, want failed", final.Status)
	}
	if h.provider.Calls() != 0 {
		t.Errorf("provider called %d times for an unpriced model under a spending budget", h.provider.Calls())
	}
	if !strings.Contains(final.Error, "cannot be enforced") {
		t.Errorf("error = %q, want it to say the budget cannot be enforced", final.Error)
	}
}

// Without a spending budget, an unpriced model still runs -- it just records
// that the cost is unknown rather than pretending it was zero.
func TestUnpricedModelRunsWhenNoSpendingBudgetIsSet(t *testing.T) {
	s := testStore(t)
	h := newHarness(t, s)
	const graph = `{"agent":"mystery2","v":1,"nodes":[
	  {"id":"n1","type":"model_call","model":"claude-not-a-real-model","prompt":"x"}]}`
	version := registerAgent(t, s, "mystery2", graph)

	st, err := s.SubmitRun(context.Background(), store.SubmitRequest{
		TenantID: "acme", AgentName: "mystery2", AgentVersion: version,
		Input: json.RawMessage(`{}`), BudgetTokens: 100000,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	runUntilDrained(t, h, []*worker.Worker{h.newWorker(t, "w-1", 1)}, 15*time.Second)

	final := loadState(t, s, st.RunID)
	if final.Status != journal.RunStatusCompleted {
		t.Fatalf("status = %q (error %s), want completed", final.Status, final.Error)
	}
	if !final.CostIncomplete {
		t.Error("CostIncomplete = false; an unpriced call makes the total a lower bound, not the cost")
	}
	if final.MicrosUsed != 0 {
		t.Errorf("MicrosUsed = %d, want 0 -- unknown, not estimated", final.MicrosUsed)
	}
}
