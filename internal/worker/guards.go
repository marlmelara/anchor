package worker

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/marlmelara/anchor/internal/ratelimit"
)

// Guards are the shared protections a worker consults before a model call.
//
// All of them are optional: a nil Guards means no rate limiting and no breaker,
// which is what the unit tests and the mock-provider benchmarks want. The
// checks are fleet-wide decisions kept in Redis, so a worker never has a
// private opinion about them.
type Guards struct {
	rdb      redis.Scripter
	provider string

	tenantCfg   ratelimit.BucketConfig
	providerPot *ratelimit.Bucket
	breaker     *ratelimit.Breaker
}

// GuardConfig configures the shared protections.
type GuardConfig struct {
	// ProviderName keys the provider bucket and the breaker.
	ProviderName string
	// PerTenant caps how fast one tenant may consume, so one noisy tenant
	// cannot spend the whole fleet's throughput.
	PerTenant ratelimit.BucketConfig
	// PerProvider caps the fleet's total rate against one provider, which is
	// the limit the provider itself actually enforces.
	PerProvider ratelimit.BucketConfig
	Breaker     ratelimit.BreakerConfig
}

// NewGuards wires the buckets and breaker to Redis.
func NewGuards(rdb redis.Scripter, cfg GuardConfig) *Guards {
	if cfg.ProviderName == "" {
		cfg.ProviderName = "unknown"
	}
	return &Guards{
		rdb:         rdb,
		provider:    cfg.ProviderName,
		tenantCfg:   cfg.PerTenant,
		providerPot: ratelimit.NewBucket(rdb, ratelimit.ProviderKey(cfg.ProviderName), cfg.PerProvider),
		breaker:     ratelimit.NewBreaker(rdb, cfg.ProviderName, cfg.Breaker),
	}
}

// Decision is what the guards concluded.
type Decision struct {
	// Allowed is whether the call may proceed.
	Allowed bool
	// RetryAfter is how long to park the run when it may not. The run is
	// REQUEUED for later rather than slept on: a sleeping goroutine holds a
	// worker slot and a lease while doing nothing, so the slot is better spent
	// on work that can proceed now.
	RetryAfter time.Duration
	// Reason names which guard refused, for the log and the metric label.
	Reason string
	// BreakerState is reported so the caller can publish it as a gauge.
	BreakerState ratelimit.State
}

// Check asks every guard whether a model call may happen.
//
// Order matters. The breaker is consulted first because a call refused by an
// open breaker should not also consume a rate-limit token -- spending a token
// on a call that will not be made would throttle the tenant for no reason.
func (g *Guards) Check(ctx context.Context, tenantID string) (Decision, error) {
	if g == nil {
		return Decision{Allowed: true}, nil
	}

	allowed, state, err := g.breaker.Allow(ctx)
	if err != nil {
		return Decision{}, err
	}
	if !allowed {
		// Park for the cooldown rather than failing the run: the provider is
		// expected to come back, and failing every queued run during a provider
		// outage would turn a transient problem into permanent data loss.
		return Decision{
			RetryAfter:   g.breaker.Cooldown(),
			Reason:       "breaker_open",
			BreakerState: state,
		}, nil
	}

	tenantBucket := ratelimit.NewBucket(g.rdb, ratelimit.TenantKey(tenantID), g.tenantCfg)
	ok, remaining, err := tenantBucket.Take(ctx)
	if err != nil {
		return Decision{}, err
	}
	if !ok {
		return Decision{
			RetryAfter:   tenantBucket.RetryAfter(remaining),
			Reason:       "tenant_rate_limit",
			BreakerState: state,
		}, nil
	}

	ok, remaining, err = g.providerPot.Take(ctx)
	if err != nil {
		return Decision{}, err
	}
	if !ok {
		return Decision{
			RetryAfter:   g.providerPot.RetryAfter(remaining),
			Reason:       "provider_rate_limit",
			BreakerState: state,
		}, nil
	}

	return Decision{Allowed: true, BreakerState: state}, nil
}

// Record folds a call result into the breaker.
func (g *Guards) Record(ctx context.Context, success bool) (ratelimit.State, error) {
	if g == nil {
		return ratelimit.StateClosed, nil
	}
	return g.breaker.Record(ctx, success)
}

// Provider names the provider these guards protect.
func (g *Guards) Provider() string {
	if g == nil {
		return "none"
	}
	return g.provider
}
