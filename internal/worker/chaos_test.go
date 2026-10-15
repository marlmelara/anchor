package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/store"
	"github.com/marlmelara/anchor/internal/testsupport"
)

// The chaos suite runs REAL worker processes and really SIGKILLs them.
//
// Nothing in-process would prove anything here. A simulated crash leaves Go's
// runtime intact: deferred functions could still run, the lease could still be
// released, state could still be flushed. SIGKILL gives the process no chance to
// do any of that, which is exactly the failure Anchor claims to survive. The
// effect being counted is a committed database row for the same reason -- an
// in-memory counter dies with the process that holds it.

// chaosGraph does one model call then one effect-recording tool call. The
// marker makes each run's effect individually identifiable.
const chaosGraph = `{
  "agent": "chaos",
  "v": 1,
  "nodes": [
    {"id": "n1", "type": "model_call", "model": "mock", "prompt": "think about {{input.marker}}"},
    {"id": "n2", "type": "tool_call", "tool": "chaos_count", "input": {"marker": "{{input.marker}}"}}
  ]
}`

// chaosEnv is a built worker binary plus the schema it runs against.
type chaosEnv struct {
	t      *testing.T
	store  *store.Store
	binary string
	dsn    string
	schema string
}

func newChaosEnv(t *testing.T) *chaosEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("chaos suite spawns processes; skipped under -short")
	}
	const schema = "test_chaos"
	s := testsupport.Store(t, schema)

	ctx := context.Background()
	if _, err := s.Pool().Exec(ctx, `DROP TABLE IF EXISTS chaos_effects`); err != nil {
		t.Fatalf("reset chaos_effects: %v", err)
	}

	binary := filepath.Join(t.TempDir(), "anchor-worker")
	build := exec.Command("go", "build", "-o", binary, "./cmd/anchor-worker")
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}

	return &chaosEnv{
		t:      t,
		store:  s,
		binary: binary,
		dsn:    os.Getenv("ANCHOR_TEST_DATABASE_URL"),
		schema: schema,
	}
}

// repoRoot walks up from the test's directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not find go.mod above the test directory")
	return ""
}

// start launches a worker process pinned to the test schema.
func (e *chaosEnv) start(id string, leaseFor time.Duration, mockLatency string) *exec.Cmd {
	e.t.Helper()
	cmd := exec.Command(e.binary,
		"-dsn", e.dsn,
		"-id", id,
		"-pool", "2",
		"-lease", leaseFor.String(),
		"-heartbeat", (leaseFor / 3).String(),
		"-mock-latency", mockLatency,
		"-chaos-tools",
		// Reap faster than the default so a killed worker's runs come back
		// inside the test's patience rather than its timeout.
		"-reap", "500ms",
		// Pin the process to the test's schema so the chaos suite cannot
		// disturb the other packages' tables.
		"-schema", e.schema,
	)
	cmd.Env = append(os.Environ(), "ANCHOR_DATABASE_URL="+e.dsn)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		e.t.Fatalf("start worker %s: %v", id, err)
	}
	return cmd
}

// kill SIGKILLs a process and reaps it. SIGKILL, not SIGTERM: the point is a
// worker that gets no chance to clean up.
func kill(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_, _ = cmd.Process.Wait()
}

func terminate(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	_, _ = cmd.Process.Wait()
}

// ---------------------------------------------------------------------------

// The M3 acceptance criterion: under a loop of worker kills, no run is lost and
// the counting tool shows zero duplicate effects.
func TestChaosLoopLosesNoRunsAndDuplicatesNoEffects(t *testing.T) {
	env := newChaosEnv(t)
	ctx := context.Background()

	version, err := env.store.RegisterAgent(ctx, "chaos", json.RawMessage(chaosGraph))
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	// The work has to outlast the chaos loop, or the suite finishes before any
	// worker is killed and the test passes without testing anything. A slow
	// mock call is what keeps runs in flight long enough to be interrupted.
	const (
		runCount     = 60
		chaosLatency = "400ms"
		killEvery    = 900 * time.Millisecond
		minKills     = 5
	)
	markers := make(map[string]uuid.UUID, runCount)
	for i := range runCount {
		marker := fmt.Sprintf("run-%02d", i)
		st, err := env.store.SubmitRun(ctx, store.SubmitRequest{
			TenantID: "chaos", AgentName: "chaos", AgentVersion: version,
			Input:        json.RawMessage(fmt.Sprintf(`{"marker":%q}`, marker)),
			BudgetTokens: 100000, BudgetMicros: 1_000_000,
		})
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		markers[marker] = st.RunID
	}

	// A short lease so a killed worker's runs come back quickly; the reaper
	// cannot act until the lease it is waiting on has actually expired.
	const leaseFor = 2 * time.Second

	var mu sync.Mutex
	workers := map[string]*exec.Cmd{}
	for i := range 3 {
		id := fmt.Sprintf("chaos-%d", i)
		workers[id] = env.start(id, leaseFor, chaosLatency)
	}
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, cmd := range workers {
			kill(cmd)
		}
	}()

	// Kill a random worker every second and immediately replace it, so the
	// fleet stays the same size while individual members keep dying.
	stopChaos := make(chan struct{})
	var kills atomic.Int64
	var chaosWG sync.WaitGroup
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		ticker := time.NewTicker(killEvery)
		defer ticker.Stop()
		for {
			select {
			case <-stopChaos:
				t.Logf("chaos loop stopped after %d kills", kills.Load())
				return
			case <-ticker.C:
				mu.Lock()
				id := fmt.Sprintf("chaos-%d", rand.N(3))
				kill(workers[id])
				workers[id] = env.start(id, leaseFor, chaosLatency)
				kills.Add(1)
				mu.Unlock()
			}
		}
	}()

	// Wait for every run to reach a terminal state.
	deadline := time.Now().Add(120 * time.Second)
	var completed int
	for time.Now().Before(deadline) {
		if err := env.store.Pool().QueryRow(ctx,
			`SELECT count(*) FROM runs WHERE status = 'completed'`).Scan(&completed); err != nil {
			t.Fatalf("count completed: %v", err)
		}
		if completed == runCount {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	close(stopChaos)
	chaosWG.Wait()

	// --- The chaos actually happened ---
	// Without this the suite can pass by finishing before a single worker is
	// killed, which proves nothing at all.
	if got := kills.Load(); got < minKills {
		t.Fatalf("only %d worker kills landed before the runs finished; the test did not exercise recovery", got)
	}

	// --- A run was genuinely taken over ---
	// This is the direct evidence of crash recovery: a RunStarted carrying
	// resumed=true means a worker picked up a run another worker had already
	// begun. Counting completions alone cannot distinguish "survived a kill"
	// from "was never interrupted".
	var resumed int
	if err := env.store.Pool().QueryRow(ctx, `
		SELECT count(*) FROM run_events
		WHERE type = 'RunStarted' AND payload->>'resumed' = 'true'`).Scan(&resumed); err != nil {
		t.Fatalf("count resumptions: %v", err)
	}
	if resumed == 0 {
		t.Errorf("no run was resumed despite %d kills; recovery was never exercised", kills.Load())
	}
	t.Logf("%d kills, %d run resumptions", kills.Load(), resumed)

	// --- No run lost ---
	if completed != runCount {
		var statuses string
		_ = env.store.Pool().QueryRow(ctx,
			`SELECT string_agg(status || '=' || n, ', ') FROM (
			   SELECT status, count(*) n FROM runs GROUP BY status) x`).Scan(&statuses)
		t.Fatalf("%d of %d runs completed under the chaos loop (statuses: %s)", completed, runCount, statuses)
	}

	// --- Zero duplicate effects ---
	// The effect table is the measurement. One row per logical step is correct;
	// two rows for one marker means a step executed twice despite the kills.
	var effectRows, distinctMarkers int
	if err := env.store.Pool().QueryRow(ctx,
		`SELECT count(*), count(DISTINCT marker) FROM chaos_effects`).Scan(&effectRows, &distinctMarkers); err != nil {
		t.Fatalf("count effects: %v", err)
	}
	if distinctMarkers != runCount {
		t.Errorf("effects recorded for %d markers, want %d; a run's effect never fired", distinctMarkers, runCount)
	}
	if effectRows != runCount {
		t.Errorf("effect fired %d times for %d runs; anything above %d is a duplicate effect under fault injection",
			effectRows, runCount, runCount)
	}

	// --- Every run's journal is still coherent ---
	for marker, runID := range markers {
		st, err := env.store.LoadState(ctx, runID)
		if err != nil {
			t.Errorf("run %s (%s): fold failed after chaos: %v", runID, marker, err)
			continue
		}
		if st.Status != journal.RunStatusCompleted {
			t.Errorf("run %s (%s): status = %q", runID, marker, st.Status)
		}
		if len(st.Steps) != 2 {
			t.Errorf("run %s (%s): %d steps, want 2", runID, marker, len(st.Steps))
		}
	}

	// --- Projections survived ---
	res, err := env.store.RebuildAll(ctx, true)
	if err != nil {
		t.Fatalf("verify projections: %v", err)
	}
	if len(res.Mismatch) != 0 {
		t.Errorf("projections drifted under chaos: %v", res.Mismatch)
	}
}

// A rolling restart -- SIGTERM, not SIGKILL -- must strand nothing. This is the
// deploy path, and it runs far more often than a crash.
func TestRollingRestartStrandsNoRuns(t *testing.T) {
	env := newChaosEnv(t)
	ctx := context.Background()

	version, err := env.store.RegisterAgent(ctx, "chaos", json.RawMessage(chaosGraph))
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	const runCount = 20
	for i := range runCount {
		if _, err := env.store.SubmitRun(ctx, store.SubmitRequest{
			TenantID: "chaos", AgentName: "chaos", AgentVersion: version,
			Input:        json.RawMessage(fmt.Sprintf(`{"marker":"roll-%02d"}`, i)),
			BudgetTokens: 100000, BudgetMicros: 1_000_000,
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	const leaseFor = 3 * time.Second
	a := env.start("roll-a", leaseFor, "100ms")
	b := env.start("roll-b", leaseFor, "100ms")
	defer func() { kill(a); kill(b) }()

	// Replace each worker in turn, the way a deploy does.
	time.Sleep(500 * time.Millisecond)
	terminate(a)
	a = env.start("roll-a", leaseFor, "100ms")
	time.Sleep(500 * time.Millisecond)
	terminate(b)
	b = env.start("roll-b", leaseFor, "100ms")

	deadline := time.Now().Add(60 * time.Second)
	var completed int
	for time.Now().Before(deadline) {
		if err := env.store.Pool().QueryRow(ctx,
			`SELECT count(*) FROM runs WHERE status = 'completed'`).Scan(&completed); err != nil {
			t.Fatalf("count completed: %v", err)
		}
		if completed == runCount {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if completed != runCount {
		t.Fatalf("%d of %d runs completed across a rolling restart", completed, runCount)
	}

	var effectRows int
	if err := env.store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM chaos_effects`).Scan(&effectRows); err != nil {
		t.Fatalf("count effects: %v", err)
	}
	if effectRows != runCount {
		t.Errorf("effect fired %d times for %d runs across a rolling restart", effectRows, runCount)
	}
}
