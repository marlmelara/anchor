package ratelimit_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/marlmelara/anchor/internal/ratelimit"
)

// These run against a real Redis. The property under test is that a Lua script
// executes without interleaving, which is a property of Redis -- a fake would
// only prove the fake serialises.
//
//	docker compose up -d
//	ANCHOR_TEST_REDIS_URL=redis://localhost:6380/0 go test ./internal/ratelimit/
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("ANCHOR_TEST_REDIS_URL")
	if url == "" {
		t.Skip("ANCHOR_TEST_REDIS_URL is not set; skipping Redis tests")
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

// uniqueKey keeps parallel test packages from sharing a bucket.
func uniqueKey(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("anchor:test:%s:%s:%d", prefix, t.Name(), time.Now().UnixNano())
}

// ---------------------------------------------------------------------------
// Token bucket
// ---------------------------------------------------------------------------

// Capacity is the burst; rate is the sustained speed. A single "limit" number
// cannot express both.
func TestBucketAllowsABurstUpToCapacity(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	b := ratelimit.NewBucket(rdb, uniqueKey(t, "burst"),
		ratelimit.BucketConfig{Capacity: 5, RefillPerSecond: 1})

	for i := range 5 {
		ok, _, err := b.Take(ctx)
		if err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("take %d denied; capacity is 5 and the bucket starts full", i)
		}
	}

	ok, remaining, err := b.Take(ctx)
	if err != nil {
		t.Fatalf("take 6: %v", err)
	}
	if ok {
		t.Error("the sixth take succeeded against a capacity of 5")
	}
	if remaining >= 1 {
		t.Errorf("remaining = %g, want under 1", remaining)
	}
}

func TestBucketRefillsOverTime(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	b := ratelimit.NewBucket(rdb, uniqueKey(t, "refill"),
		ratelimit.BucketConfig{Capacity: 2, RefillPerSecond: 10})

	for range 2 {
		if ok, _, _ := b.Take(ctx); !ok {
			t.Fatal("initial take denied")
		}
	}
	if ok, _, _ := b.Take(ctx); ok {
		t.Fatal("took a third token from a capacity of 2")
	}

	// At 10/sec, 150ms is comfortably more than one token.
	time.Sleep(150 * time.Millisecond)

	ok, _, err := b.Take(ctx)
	if err != nil {
		t.Fatalf("take after refill: %v", err)
	}
	if !ok {
		t.Error("the bucket did not refill")
	}
}

// The reason the whole take is one Lua script. With read-then-write as separate
// commands, concurrent callers would both see the last token and both take it.
func TestBucketIsAtomicUnderConcurrency(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	const capacity = 20
	b := ratelimit.NewBucket(rdb, uniqueKey(t, "atomic"),
		// A refill rate low enough that nothing meaningful is added during the test.
		ratelimit.BucketConfig{Capacity: capacity, RefillPerSecond: 0.001})

	var mu sync.Mutex
	granted := 0
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := b.Take(ctx)
			if err != nil {
				t.Errorf("take: %v", err)
				return
			}
			if ok {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if granted != capacity {
		t.Errorf("granted %d tokens from a capacity of %d; the take is not atomic", granted, capacity)
	}
}

// An unused bucket starts full, so a tenant is not punished for having been
// quiet.
func TestBucketStartsFull(t *testing.T) {
	rdb := testRedis(t)
	b := ratelimit.NewBucket(rdb, uniqueKey(t, "fresh"),
		ratelimit.BucketConfig{Capacity: 3, RefillPerSecond: 1})
	ok, remaining, err := b.Take(context.Background())
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if !ok {
		t.Error("the first take on a fresh bucket was denied")
	}
	if remaining < 1.9 {
		t.Errorf("remaining = %g after one take from capacity 3, want about 2", remaining)
	}
}

// RetryAfter decides available_at when a run is requeued, so it comes back when
// it can actually proceed rather than re-failing the same check immediately.
func TestRetryAfterScalesWithTheRefillRate(t *testing.T) {
	rdb := testRedis(t)
	slow := ratelimit.NewBucket(rdb, uniqueKey(t, "slow"),
		ratelimit.BucketConfig{Capacity: 1, RefillPerSecond: 1})
	fast := ratelimit.NewBucket(rdb, uniqueKey(t, "fast"),
		ratelimit.BucketConfig{Capacity: 1, RefillPerSecond: 100})

	if slow.RetryAfter(0) <= fast.RetryAfter(0) {
		t.Errorf("slow bucket retry %s should exceed fast bucket retry %s",
			slow.RetryAfter(0), fast.RetryAfter(0))
	}
	if got := slow.RetryAfter(1); got != 0 {
		t.Errorf("RetryAfter with a token available = %s, want 0", got)
	}
}

// A rate limit protects someone else's service, so it fails CLOSED. The
// opposite of the breaker below.
func TestBucketFailsClosedWhenRedisIsUnreachable(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = dead.Close() }()

	b := ratelimit.NewBucket(dead, "anchor:test:dead",
		ratelimit.BucketConfig{Capacity: 10, RefillPerSecond: 1})
	ok, _, err := b.Take(context.Background())
	if ok {
		t.Error("the bucket allowed a call while Redis was down; a rate limit must fail closed")
	}
	if err == nil {
		t.Error("no error reported for an unreachable Redis")
	}
}

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------

func newBreaker(t *testing.T, rdb redis.Scripter, cfg ratelimit.BreakerConfig) *ratelimit.Breaker {
	t.Helper()
	b := ratelimit.NewBreaker(rdb, fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano()), cfg)
	t.Cleanup(func() { _ = b.Reset(context.Background()) })
	return b
}

// The M5 acceptance criterion: errors trip the breaker open, then half-open,
// then closed.
func TestBreakerGoesClosedToOpenToHalfOpenToClosed(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	b := newBreaker(t, rdb, ratelimit.BreakerConfig{
		Window: 10 * time.Second, MinCalls: 4, ErrorRateToTrip: 0.5,
		Cooldown: 300 * time.Millisecond,
	})

	// --- closed ---
	allowed, state, err := b.Allow(ctx)
	if err != nil {
		t.Fatalf("allow: %v", err)
	}
	if !allowed || state != ratelimit.StateClosed {
		t.Fatalf("fresh breaker: allowed=%v state=%q, want true/closed", allowed, state)
	}

	// --- closed -> open ---
	// Four failures out of four is a 100% error rate over the minimum calls.
	for i := range 4 {
		if _, err := b.Record(ctx, false); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	snap, err := b.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.State != ratelimit.StateOpen {
		t.Fatalf("state = %q after 4 failures, want open", snap.State)
	}

	// --- open: fail fast, do not call the provider ---
	allowed, state, err = b.Allow(ctx)
	if err != nil {
		t.Fatalf("allow while open: %v", err)
	}
	if allowed {
		t.Error("the breaker allowed a call while open; the point is to fail fast")
	}
	if state != ratelimit.StateOpen {
		t.Errorf("state = %q, want open", state)
	}

	// --- open -> half-open after the cooldown ---
	time.Sleep(400 * time.Millisecond)
	allowed, state, err = b.Allow(ctx)
	if err != nil {
		t.Fatalf("allow after cooldown: %v", err)
	}
	if !allowed || state != ratelimit.StateHalfOpen {
		t.Fatalf("after cooldown: allowed=%v state=%q, want true/half-open", allowed, state)
	}

	// --- half-open -> closed on a successful probe ---
	if _, err := b.Record(ctx, true); err != nil {
		t.Fatalf("record probe: %v", err)
	}
	snap, err = b.Inspect(ctx)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.State != ratelimit.StateClosed {
		t.Errorf("state = %q after a successful probe, want closed", snap.State)
	}
}

// A failed probe reopens the breaker and restarts the cooldown, rather than
// letting the fleet back in against a provider that is still down.
func TestBreakerReopensOnAFailedProbe(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	b := newBreaker(t, rdb, ratelimit.BreakerConfig{
		Window: 10 * time.Second, MinCalls: 2, ErrorRateToTrip: 0.5,
		Cooldown: 200 * time.Millisecond,
	})

	for range 2 {
		_, _ = b.Record(ctx, false)
	}
	time.Sleep(300 * time.Millisecond)

	if allowed, _, _ := b.Allow(ctx); !allowed {
		t.Fatal("no probe was admitted after the cooldown")
	}
	if _, err := b.Record(ctx, false); err != nil {
		t.Fatalf("record failed probe: %v", err)
	}

	snap, _ := b.Inspect(ctx)
	if snap.State != ratelimit.StateOpen {
		t.Errorf("state = %q after a failed probe, want open", snap.State)
	}
	if allowed, _, _ := b.Allow(ctx); allowed {
		t.Error("a call was allowed immediately after a failed probe; the cooldown must restart")
	}
}

// Exactly one caller may probe. Letting the whole fleet through at once would
// re-flood a service that is still fragile.
func TestBreakerAdmitsExactlyOneProbe(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	b := newBreaker(t, rdb, ratelimit.BreakerConfig{
		Window: 10 * time.Second, MinCalls: 2, ErrorRateToTrip: 0.5,
		Cooldown: 200 * time.Millisecond,
	})

	for range 2 {
		_, _ = b.Record(ctx, false)
	}
	time.Sleep(300 * time.Millisecond)

	var mu sync.Mutex
	admitted := 0
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _, err := b.Allow(ctx); err == nil && ok {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if admitted != 1 {
		t.Errorf("%d callers were admitted as the probe, want exactly 1", admitted)
	}
}

// One failure out of one is a 100% error rate. Without a minimum call count the
// breaker would trip on a single blip.
func TestBreakerDoesNotTripBelowTheMinimumCallCount(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	b := newBreaker(t, rdb, ratelimit.BreakerConfig{
		Window: 10 * time.Second, MinCalls: 20, ErrorRateToTrip: 0.5,
		Cooldown: time.Second,
	})

	for range 5 {
		_, _ = b.Record(ctx, false)
	}
	snap, _ := b.Inspect(ctx)
	if snap.State != ratelimit.StateClosed {
		t.Errorf("state = %q after 5 failures with a 20-call minimum, want closed", snap.State)
	}
	if allowed, _, _ := b.Allow(ctx); !allowed {
		t.Error("calls were blocked below the minimum call count")
	}
}

// Mostly-successful traffic must not trip the breaker.
func TestBreakerStaysClosedBelowTheErrorThreshold(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	b := newBreaker(t, rdb, ratelimit.BreakerConfig{
		Window: 10 * time.Second, MinCalls: 10, ErrorRateToTrip: 0.5,
		Cooldown: time.Second,
	})

	for range 8 {
		_, _ = b.Record(ctx, true)
	}
	for range 2 {
		_, _ = b.Record(ctx, false)
	}
	snap, _ := b.Inspect(ctx)
	if snap.State != ratelimit.StateClosed {
		t.Errorf("state = %q at a 20%% error rate against a 50%% threshold, want closed", snap.State)
	}
	if snap.OK != 8 || snap.Failures != 2 {
		t.Errorf("counters = %d ok / %d failed, want 8/2", snap.OK, snap.Failures)
	}
}

// The breaker is an optimisation, not a promise: a Redis outage must not halt
// every model call. This is the deliberate opposite of the rate limiter.
func TestBreakerFailsOpenWhenRedisIsUnreachable(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = dead.Close() }()

	b := ratelimit.NewBreaker(dead, "unreachable", ratelimit.BreakerConfig{FailOpen: true})
	allowed, _, err := b.Allow(context.Background())
	if !allowed {
		t.Error("the breaker blocked a call while Redis was down; it must fail open")
	}
	if err != nil {
		t.Errorf("fail-open should not surface an error: %v", err)
	}
}
