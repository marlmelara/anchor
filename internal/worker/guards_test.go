package worker_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/marlmelara/anchor/internal/ratelimit"
	"github.com/marlmelara/anchor/internal/worker"
)

func guardRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("ANCHOR_TEST_REDIS_URL")
	if url == "" {
		t.Skip("ANCHOR_TEST_REDIS_URL is not set; skipping guard tests")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	c := redis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis unreachable: %v", err)
	}
	return c
}

// The M5 acceptance criterion, through the worker: a provider returning errors
// trips the breaker, and while it is open the worker PARKS runs instead of
// attempting them.
//
// The open -> half-open -> closed state machine itself is tested in
// internal/ratelimit, where it belongs. What this adds is the worker's
// behaviour on refusal: park, do not fail. Failing every queued run during a
// provider outage would turn a transient problem into permanent loss.
//
// The cooldown is deliberately LONG here. With a short one the breaker
// correctly goes half-open mid-test and admits probes, which is right but makes
// "no calls while open" unassertable.
func TestBreakerTripsThroughTheWorkerAndParksRuns(t *testing.T) {
	s := testStore(t)
	rdb := guardRedis(t)
	ctx := context.Background()
	h := newHarness(t, s)

	providerName := fmt.Sprintf("mock-%d", time.Now().UnixNano())
	breakerCfg := ratelimit.BreakerConfig{
		Window: 30 * time.Second, MinCalls: 3, ErrorRateToTrip: 0.5,
		Cooldown: 30 * time.Second, FailOpen: true,
	}
	breaker := ratelimit.NewBreaker(rdb, providerName, breakerCfg)
	t.Cleanup(func() { _ = breaker.Reset(ctx) })

	guards := worker.NewGuards(rdb, worker.GuardConfig{
		ProviderName: providerName,
		// Generous limits so only the breaker can refuse anything here.
		PerTenant:   ratelimit.BucketConfig{Capacity: 1000, RefillPerSecond: 1000},
		PerProvider: ratelimit.BucketConfig{Capacity: 1000, RefillPerSecond: 1000},
		Breaker:     breakerCfg,
	})

	// Every call fails: what a provider outage looks like.
	h.provider.FailFirst = 1000

	version := registerAgent(t, s, "two-step", twoNodeGraph)
	submit(t, s, "two-step", version, `{"topic":"outage"}`)

	newWorker := func(id string) *worker.Worker {
		return worker.New(h.store, h.provider, h.registry, h.runner, worker.Config{
			WorkerID: id, PoolSize: 1,
			LeaseFor: 10 * time.Second, HeartbeatEvery: time.Second,
			IdlePoll: 5 * time.Millisecond, BaseBackoff: time.Millisecond,
			MaxAttempts: 3, Guards: guards,
			Logf: func(format string, args ...any) { t.Logf(format, args...) },
		})
	}
	runUntilDrained(t, h, []*worker.Worker{newWorker("w-breaker")}, 20*time.Second)

	// --- closed -> open ---
	snap, err := breaker.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.State != ratelimit.StateOpen {
		t.Fatalf("breaker state = %q after a failing provider, want open (ok=%d fail=%d)",
			snap.State, snap.OK, snap.Failures)
	}

	// --- open: a new run is parked, and the provider is never called ---
	callsBefore := h.provider.Calls()
	parked := submit(t, s, "two-step", version, `{"topic":"while-open"}`)
	runUntilDrained(t, h, []*worker.Worker{newWorker("w-breaker-2")}, 3*time.Second)

	parkedState := loadState(t, s, parked)
	if parkedState.Terminal() {
		t.Errorf("a run submitted while the breaker was open reached %q; it should be parked for retry",
			parkedState.Status)
	}
	if got := h.provider.Calls() - callsBefore; got != 0 {
		t.Errorf("the provider was called %d times while the breaker was open; the point is to fail fast", got)
	}

	// The parked run is back on the queue with no lease held.
	var queued, leased int
	if err := s.Pool().QueryRow(ctx,
		`SELECT (SELECT count(*) FROM queue WHERE run_id = $1),
		        (SELECT count(*) FROM leases WHERE run_id = $1)`, parked).Scan(&queued, &leased); err != nil {
		t.Fatalf("count queue/leases: %v", err)
	}
	if queued != 1 || leased != 0 {
		t.Errorf("parked run: queued=%d leased=%d, want 1/0", queued, leased)
	}
}

// An empty bucket must PARK the run, not fail it and not sleep on it. A
// sleeping goroutine holds a worker slot and a lease while doing nothing.
func TestRateLimitParksTheRunRatherThanFailingIt(t *testing.T) {
	s := testStore(t)
	rdb := guardRedis(t)
	h := newHarness(t, s)

	providerName := fmt.Sprintf("mock-rl-%d", time.Now().UnixNano())
	guards := worker.NewGuards(rdb, worker.GuardConfig{
		ProviderName: providerName,
		// A bucket with no capacity refuses everything, which is the cleanest
		// way to exercise the deferral path.
		PerTenant:   ratelimit.BucketConfig{Capacity: 0.5, RefillPerSecond: 0.001},
		PerProvider: ratelimit.BucketConfig{Capacity: 1000, RefillPerSecond: 1000},
		Breaker:     ratelimit.BreakerConfig{FailOpen: true},
	})

	version := registerAgent(t, s, "two-step", twoNodeGraph)
	runID := submit(t, s, "two-step", version, `{"topic":"throttled"}`)

	w := worker.New(h.store, h.provider, h.registry, h.runner, worker.Config{
		WorkerID: "w-rl", PoolSize: 1,
		LeaseFor: 10 * time.Second, HeartbeatEvery: time.Second,
		IdlePoll: 5 * time.Millisecond, Guards: guards,
		Logf: func(format string, args ...any) { t.Logf(format, args...) },
	})
	runUntilDrained(t, h, []*worker.Worker{w}, 5*time.Second)

	st := loadState(t, s, runID)
	if st.Terminal() {
		t.Errorf("status = %q; a rate-limited run must be parked for retry, not failed", st.Status)
	}
	if h.provider.Calls() != 0 {
		t.Errorf("the provider was called %d times despite an empty bucket", h.provider.Calls())
	}

	// Parked means back on the queue with a future available_at, and NOT
	// holding a lease -- the worker slot has to be free for other work.
	var queued, leased int
	if err := s.Pool().QueryRow(context.Background(),
		`SELECT (SELECT count(*) FROM queue WHERE run_id = $1),
		        (SELECT count(*) FROM leases WHERE run_id = $1)`, runID).Scan(&queued, &leased); err != nil {
		t.Fatalf("count queue/leases: %v", err)
	}
	if queued != 1 {
		t.Errorf("queue rows = %d, want 1; the run was not requeued", queued)
	}
	if leased != 0 {
		t.Errorf("lease rows = %d, want 0; a parked run must not hold a lease", leased)
	}
}

// With no Redis configured, the worker runs unrestricted. That is what the
// mock-provider benchmarks need, so the numbers measure Anchor alone.
func TestNilGuardsAllowEverything(t *testing.T) {
	var guards *worker.Guards
	decision, err := guards.Check(context.Background(), "acme")
	if err != nil {
		t.Fatalf("Check on nil guards: %v", err)
	}
	if !decision.Allowed {
		t.Error("nil guards refused a call; absent protections must not block work")
	}
	if state, err := guards.Record(context.Background(), false); err != nil || state != ratelimit.StateClosed {
		t.Errorf("Record on nil guards = %q, %v", state, err)
	}
}
