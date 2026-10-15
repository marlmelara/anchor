// Package ratelimit holds the two pieces of shared protection that live in
// Redis: a token bucket that caps how fast work may be done, and a circuit
// breaker that stops hammering a provider that is already failing.
//
// Both keep their state in Redis rather than in each worker, because both are
// fleet-wide decisions. A per-worker rate limit of ten per second across twelve
// workers is a limit of a hundred and twenty per second, which is not the limit
// anyone asked for.
//
// Losing this state is harmless by design: a bucket refills and a breaker
// re-learns. That is why Redis is acceptable here and why the journal is not in
// it.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// takeScript is a token bucket: lazy refill, then take one token if available.
//
// It is a script because the whole sequence -- read, refill, test, take, write
// -- has to be atomic. Issued as separate commands from Go, two workers could
// both read "one token left" and both take it. Redis runs a script to
// completion without interleaving anything else, which is the cheapest
// mutual exclusion available here.
//
// The clock comes from Redis's own TIME, not from the caller. A worker's clock
// is the wrong source: skew between workers would make the same bucket refill
// at different apparent rates depending on who asked.
var takeScript = redis.NewScript(`
local key  = KEYS[1]
local cap  = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local b      = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts     = tonumber(b[2])
if tokens == nil then
  -- A bucket nobody has used starts full, so a quiet tenant is not punished
  -- for having been quiet.
  tokens = cap
  ts = now
end

-- Lazy refill: rather than a timer adding tokens on a schedule, work out how
-- many WOULD have been added since the last call. Equivalent, and it costs
-- nothing while a bucket is idle.
local elapsed = math.max(0, now - ts) / 1000
tokens = math.min(cap, tokens + elapsed * rate)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- Expire idle buckets so a tenant seen once does not occupy memory forever.
-- Twice the time it takes to refill from empty, so an expiry can never discard
-- a bucket that was still being drawn down.
redis.call('PEXPIRE', key, math.ceil(cap / rate * 1000) * 2 + 1000)

return {allowed, tostring(tokens)}
`)

// Bucket is one token bucket.
type Bucket struct {
	rdb redis.Scripter
	key string
	cfg BucketConfig
}

// BucketConfig describes the shape of a limit.
type BucketConfig struct {
	// Capacity is the burst allowance: how many calls may happen at once after
	// a quiet period.
	Capacity float64
	// RefillPerSecond is the sustained rate.
	//
	// Capacity and rate answer different questions. Capacity 10 with rate 2
	// allows ten calls immediately, then two per second. A single "rate limit"
	// number cannot express that.
	RefillPerSecond float64
	// FailOpen decides what happens when Redis itself is unreachable.
	//
	// A rate limit is a protection for someone else's service, so the safe
	// default is to fail CLOSED -- deny -- rather than flood a provider during
	// a Redis outage. This is the opposite of the circuit breaker, which is an
	// optimisation for us and fails open.
	FailOpen bool
}

// NewBucket builds a bucket for a key.
func NewBucket(rdb redis.Scripter, key string, cfg BucketConfig) *Bucket {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 10
	}
	if cfg.RefillPerSecond <= 0 {
		cfg.RefillPerSecond = 5
	}
	return &Bucket{rdb: rdb, key: key, cfg: cfg}
}

// TenantKey is the bucket key for one tenant.
func TenantKey(tenantID string) string { return "anchor:rl:tenant:" + tenantID }

// ProviderKey is the bucket key for one provider.
func ProviderKey(provider string) string { return "anchor:rl:provider:" + provider }

// Take attempts to spend one token.
//
// Returns false when the bucket is empty. The caller must NOT sleep on that: a
// sleeping goroutine holds a worker slot and a lease while doing nothing. The
// run is requeued with a future available_at instead, which frees the slot for
// work that can actually proceed.
func (b *Bucket) Take(ctx context.Context) (bool, float64, error) {
	res, err := takeScript.Run(ctx, b.rdb, []string{b.key},
		b.cfg.Capacity, b.cfg.RefillPerSecond, 1).Slice()
	if err != nil {
		if b.cfg.FailOpen {
			return true, 0, nil
		}
		return false, 0, fmt.Errorf("take from bucket %s: %w", b.key, err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("bucket %s returned %d values, want 2", b.key, len(res))
	}

	allowed, _ := res[0].(int64)
	remaining := 0.0
	if s, ok := res[1].(string); ok {
		_, _ = fmt.Sscanf(s, "%g", &remaining)
	}
	return allowed == 1, remaining, nil
}

// RetryAfter is how long until the bucket holds a token again.
//
// Used to set available_at when a run is requeued, so it comes back when it can
// actually proceed rather than immediately re-failing the same check.
func (b *Bucket) RetryAfter(remaining float64) time.Duration {
	needed := 1 - remaining
	if needed <= 0 {
		return 0
	}
	seconds := needed / b.cfg.RefillPerSecond
	d := time.Duration(seconds * float64(time.Second))
	if d < 50*time.Millisecond {
		return 50 * time.Millisecond
	}
	return d
}
