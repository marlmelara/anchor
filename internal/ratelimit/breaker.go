package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// State is a circuit breaker's position.
type State string

const (
	// StateClosed is normal: calls pass through.
	StateClosed State = "closed"
	// StateOpen means the provider is failing, so calls fail fast without
	// being attempted. This protects both sides -- the worker stops spending
	// time and money on doomed calls, and the provider gets room to recover.
	StateOpen State = "open"
	// StateHalfOpen admits exactly ONE probe to find out whether the provider
	// came back. Letting the whole fleet through at once would re-flood a
	// service that is still fragile.
	StateHalfOpen State = "half-open"
)

// BreakerConfig tunes one breaker.
type BreakerConfig struct {
	// Window is the sliding period over which the error rate is measured.
	Window time.Duration
	// MinCalls is how many calls must land in the window before the rate means
	// anything. Without it, one failed call out of one is a 100% error rate and
	// would trip the breaker on a single blip.
	MinCalls int64
	// ErrorRateToTrip is the failure fraction that opens the breaker, 0..1.
	ErrorRateToTrip float64
	// Cooldown is how long the breaker stays open before admitting a probe.
	Cooldown time.Duration
	// FailOpen decides what happens when Redis is unreachable.
	//
	// Defaults to true, and the asymmetry with the rate limiter is deliberate:
	// a breaker is an OPTIMISATION -- it saves time and money on calls likely
	// to fail -- so a Redis outage should not halt all work. A budget or a rate
	// limit is a PROMISE, and a promise fails closed.
	FailOpen bool
}

func (c *BreakerConfig) setDefaults() {
	if c.Window <= 0 {
		c.Window = 60 * time.Second
	}
	if c.MinCalls <= 0 {
		c.MinCalls = 20
	}
	if c.ErrorRateToTrip <= 0 {
		c.ErrorRateToTrip = 0.5
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 30 * time.Second
	}
}

// allowScript decides whether a call may proceed, and claims the probe slot
// when the breaker is ready to test recovery.
//
// Atomic because the half-open probe is a race by nature: every worker reaches
// the end of the cooldown at the same moment, and exactly one must be let
// through. The SET-if-absent on the probe key is a tiny distributed lock.
var allowScript = redis.NewScript(`
local key      = KEYS[1]
local probeKey = KEYS[2]
local cooldown = tonumber(ARGV[1])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local st       = redis.call('HGET', key, 'state') or 'closed'
local openedAt = tonumber(redis.call('HGET', key, 'opened_at')) or 0

if st ~= 'open' and st ~= 'half-open' then
  return {1, 'closed'}
end

if st == 'open' then
  if now - openedAt < cooldown then
    return {0, 'open'}
  end
  -- Cooldown elapsed: move to half-open and let this caller be the probe.
  redis.call('HSET', key, 'state', 'half-open')
  redis.call('SET', probeKey, '1', 'PX', cooldown, 'NX')
  return {1, 'half-open'}
end

-- Already half-open: only the caller that wins the probe key proceeds.
local won = redis.call('SET', probeKey, '1', 'PX', cooldown, 'NX')
if won then
  return {1, 'half-open'}
end
return {0, 'half-open'}
`)

// recordScript folds one result into the window and moves the state if needed.
var recordScript = redis.NewScript(`
local key      = KEYS[1]
local probeKey = KEYS[2]
local success   = tonumber(ARGV[1])
local windowMs  = tonumber(ARGV[2])
local minCalls  = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local st        = redis.call('HGET', key, 'state') or 'closed'
local windowFrom = tonumber(redis.call('HGET', key, 'window_from')) or now

-- A probe result decides the breaker outright; the window does not get a vote.
if st == 'half-open' then
  redis.call('DEL', probeKey)
  if success == 1 then
    redis.call('HSET', key, 'state', 'closed', 'ok', 0, 'fail', 0, 'window_from', now)
    redis.call('HDEL', key, 'opened_at')
    return 'closed'
  end
  redis.call('HSET', key, 'state', 'open', 'opened_at', now, 'ok', 0, 'fail', 0, 'window_from', now)
  return 'open'
end

-- Tumbling window: once the period is over, start counting again. Simpler than
-- a true sliding window and good enough to spot a provider that is failing --
-- the cost is that a burst straddling the boundary is split across two windows.
if now - windowFrom >= windowMs then
  redis.call('HSET', key, 'ok', 0, 'fail', 0, 'window_from', now)
end

if success == 1 then
  redis.call('HINCRBY', key, 'ok', 1)
else
  redis.call('HINCRBY', key, 'fail', 1)
end

local ok   = tonumber(redis.call('HGET', key, 'ok')) or 0
local fail = tonumber(redis.call('HGET', key, 'fail')) or 0
local total = ok + fail

if st == 'closed' and total >= minCalls and (fail / total) > threshold then
  redis.call('HSET', key, 'state', 'open', 'opened_at', now)
  redis.call('PEXPIRE', key, windowMs * 10)
  return 'open'
end

redis.call('PEXPIRE', key, windowMs * 10)
return st
`)

// Breaker is one per-provider circuit breaker.
type Breaker struct {
	rdb      redis.Scripter
	key      string
	probeKey string
	cfg      BreakerConfig
}

// NewBreaker builds a breaker for a provider.
func NewBreaker(rdb redis.Scripter, provider string, cfg BreakerConfig) *Breaker {
	cfg.setDefaults()
	return &Breaker{
		rdb:      rdb,
		key:      "anchor:cb:" + provider,
		probeKey: "anchor:cb:" + provider + ":probe",
		cfg:      cfg,
	}
}

// Allow reports whether a call may be attempted, and the state it was in.
func (b *Breaker) Allow(ctx context.Context) (bool, State, error) {
	res, err := allowScript.Run(ctx, b.rdb,
		[]string{b.key, b.probeKey}, b.cfg.Cooldown.Milliseconds()).Slice()
	if err != nil {
		if b.cfg.FailOpen {
			// A Redis problem must not stop all model calls. The breaker saves
			// money; it is not what keeps the system correct.
			return true, StateClosed, nil
		}
		return false, StateOpen, fmt.Errorf("breaker %s: %w", b.key, err)
	}
	if len(res) != 2 {
		return true, StateClosed, fmt.Errorf("breaker %s returned %d values, want 2", b.key, len(res))
	}
	allowed, _ := res[0].(int64)
	state, _ := res[1].(string)
	return allowed == 1, State(state), nil
}

// Record folds one call result into the breaker.
func (b *Breaker) Record(ctx context.Context, success bool) (State, error) {
	flag := 0
	if success {
		flag = 1
	}
	res, err := recordScript.Run(ctx, b.rdb, []string{b.key, b.probeKey},
		flag, b.cfg.Window.Milliseconds(), b.cfg.MinCalls, b.cfg.ErrorRateToTrip).Text()
	if err != nil {
		if b.cfg.FailOpen {
			return StateClosed, nil
		}
		return StateOpen, fmt.Errorf("breaker %s record: %w", b.key, err)
	}
	return State(res), nil
}

// Cooldown is how long the breaker stays open before admitting a probe. The
// worker parks a refused run for this long, so it comes back when the breaker
// is at least willing to try again.
func (b *Breaker) Cooldown() time.Duration { return b.cfg.Cooldown }

// Snapshot is a breaker's current state, for metrics and tests.
type Snapshot struct {
	State    State
	OK       int64
	Failures int64
}

// Inspect reads the breaker without changing it.
func (b *Breaker) Inspect(ctx context.Context) (Snapshot, error) {
	client, ok := b.rdb.(redis.Cmdable)
	if !ok {
		return Snapshot{}, fmt.Errorf("breaker %s: client does not support reads", b.key)
	}
	vals, err := client.HMGet(ctx, b.key, "state", "ok", "fail").Result()
	if err != nil {
		return Snapshot{}, fmt.Errorf("inspect breaker %s: %w", b.key, err)
	}
	snap := Snapshot{State: StateClosed}
	if s, ok := vals[0].(string); ok && s != "" {
		snap.State = State(s)
	}
	if s, ok := vals[1].(string); ok {
		_, _ = fmt.Sscanf(s, "%d", &snap.OK)
	}
	if s, ok := vals[2].(string); ok {
		_, _ = fmt.Sscanf(s, "%d", &snap.Failures)
	}
	return snap, nil
}

// Reset clears a breaker. Operator tooling and tests only.
func (b *Breaker) Reset(ctx context.Context) error {
	client, ok := b.rdb.(redis.Cmdable)
	if !ok {
		return fmt.Errorf("breaker %s: client does not support deletes", b.key)
	}
	return client.Del(ctx, b.key, b.probeKey).Err()
}
