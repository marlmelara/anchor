package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/provider"
	"github.com/marlmelara/anchor/internal/store"
	"github.com/marlmelara/anchor/internal/tool"
	"github.com/marlmelara/anchor/internal/worker"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("ANCHOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ANCHOR_TEST_DATABASE_URL is not set; skipping database tests")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 24})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	if _, err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := s.Pool().Exec(ctx,
		`TRUNCATE queue, leases, steps, run_events, runs, agents RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
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
		Input: json.RawMessage(input), BudgetTokens: 100000, BudgetCents: 1000,
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
		provider.ToolReply("echo", json.RawMessage(`{"q":"hello"}`), 10, 5, 1),
		provider.TextReply("all done", 12, 6, 1),
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
