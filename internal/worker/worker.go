// Package worker claims runs and executes them.
//
// The shape is a pool of goroutines, each looping claim -> execute -> journal ->
// release. One goroutine owns one run at a time, which makes every run
// single-writer: there is never a question of two parts of the system changing
// one run's state concurrently, so no ordering conflicts exist to resolve. The
// cost is that independent steps within a run cannot run in parallel.
package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/marlmelara/anchor/internal/agent"
	"github.com/marlmelara/anchor/internal/journal"
	"github.com/marlmelara/anchor/internal/metrics"
	"github.com/marlmelara/anchor/internal/provider"
	"github.com/marlmelara/anchor/internal/store"
	"github.com/marlmelara/anchor/internal/tool"
)

// Config tunes a worker process.
type Config struct {
	// WorkerID identifies this process in leases. It must be unique across the
	// fleet, or two processes could renew each other's leases.
	WorkerID string
	// PoolSize is how many runs this process executes concurrently. It is also
	// the backpressure mechanism: a goroutine claims only when it is free, so a
	// worker never takes on more runs than it can execute.
	PoolSize int

	LeaseFor       time.Duration
	HeartbeatEvery time.Duration
	// IdlePoll is how long to wait before polling again when the queue is empty.
	IdlePoll time.Duration
	// ReapEvery is how often this worker looks for leases whose owner stopped
	// heartbeating. Every worker reaps, so recovery does not depend on one
	// designated process staying alive.
	ReapEvery time.Duration

	// MaxAttempts caps retries per step.
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration

	// ModelCallTimeout bounds a single model call.
	ModelCallTimeout time.Duration
	// MaxTokensPerCall is the per-call ceiling and the figure used to project
	// cost against the remaining budget before a call is made.
	MaxTokensPerCall int

	Logf func(format string, args ...any)

	// Guards are the shared rate limits and circuit breaker. Nil disables both,
	// which is what unit tests and mock-provider benchmarks want.
	Guards *Guards
	// Metrics is optional; nil means no instrumentation.
	Metrics *metrics.Metrics
}

func (c *Config) setDefaults() {
	if c.PoolSize <= 0 {
		c.PoolSize = 4
	}
	if c.LeaseFor <= 0 {
		c.LeaseFor = 30 * time.Second
	}
	if c.HeartbeatEvery <= 0 {
		c.HeartbeatEvery = 10 * time.Second
	}
	if c.IdlePoll <= 0 {
		c.IdlePoll = 100 * time.Millisecond
	}
	if c.ReapEvery <= 0 {
		c.ReapEvery = 5 * time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 200 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 10 * time.Second
	}
	if c.ModelCallTimeout <= 0 {
		c.ModelCallTimeout = 90 * time.Second
	}
	if c.MaxTokensPerCall <= 0 {
		c.MaxTokensPerCall = 1024
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}

// Worker is one process's pool.
type Worker struct {
	store    *store.Store
	provider provider.Provider
	tools    *tool.Registry
	runner   tool.Runner
	cfg      Config
}

// observe is a nil-safe metrics helper, so every call site is one line rather
// than one line wrapped in a nil check.
func (w *Worker) observe(fn func(m *metrics.Metrics)) {
	if w.cfg.Metrics != nil {
		fn(w.cfg.Metrics)
	}
}

func New(s *store.Store, p provider.Provider, tools *tool.Registry, runner tool.Runner, cfg Config) *Worker {
	cfg.setDefaults()
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-" + uuid.NewString()[:8]
	}
	return &Worker{
		store:    s,
		provider: p,
		tools:    tools,
		runner:   runner,
		cfg:      cfg,
	}
}

// ID is this worker's lease identity.
func (w *Worker) ID() string { return w.cfg.WorkerID }

// Run starts the pool and blocks until claimCtx is cancelled and all in-flight
// runs have finished.
//
// Two contexts make shutdown graceful. claimCtx is cancelled on SIGTERM and
// stops goroutines taking NEW runs. runCtx stays alive so work already in
// flight can finish and journal its result, instead of being abandoned
// mid-step for another worker to redo.
func (w *Worker) Run(claimCtx, runCtx context.Context) error {
	g, claimCtx := errgroup.WithContext(claimCtx)

	// The reaper is what turns a dead worker's runs back into queued work. It
	// is a peer of the claim loops, not a separate service, so there is no
	// single process whose death stops recovery.
	g.Go(func() error {
		ticker := time.NewTicker(w.cfg.ReapEvery)
		defer ticker.Stop()
		for {
			select {
			case <-claimCtx.Done():
				return nil
			case <-ticker.C:
				n, err := w.store.ReapExpiredLeases(claimCtx)
				if err != nil && claimCtx.Err() == nil {
					w.cfg.Logf("reap: %v", err)
					continue
				}
				if n > 0 {
					w.cfg.Logf("reaped %d run(s) from expired leases", n)
					w.observe(func(m *metrics.Metrics) { m.LeasesReaped.Add(float64(n)) })
				}
				if depth, derr := w.store.QueueDepth(claimCtx); derr == nil {
					w.observe(func(m *metrics.Metrics) { m.QueueDepth.Set(float64(depth)) })
				}
			}
		}
	})

	for i := 0; i < w.cfg.PoolSize; i++ {
		g.Go(func() error {
			for claimCtx.Err() == nil {
				runID, err := w.store.ClaimNext(claimCtx, w.cfg.WorkerID, w.cfg.LeaseFor)
				switch {
				case errors.Is(err, store.ErrNoWork):
					if !sleepCtx(claimCtx, w.cfg.IdlePoll) {
						return nil
					}
					continue
				case errors.Is(err, context.Canceled):
					return nil
				case err != nil:
					return fmt.Errorf("claim: %w", err)
				}

				// A run's own failure is journaled, never returned: one bad run
				// must not take down the goroutine that would execute the next
				// hundred.
				w.executeRun(runCtx, runID)
			}
			return nil
		})
	}
	return g.Wait()
}

// sleepCtx waits for d, or returns false if the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// executeRun takes a claimed run as far as it can.
func (w *Worker) executeRun(parent context.Context, runID uuid.UUID) {
	// This run's own context. Cancelling it aborts in-flight model calls and
	// tool containers, which is how both user cancellation and a lost lease
	// stop work that is already running.
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	released := false
	defer func() {
		if !released {
			// A fresh context: the run's own may already be cancelled, and the
			// lease still has to be given back.
			releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
			defer releaseCancel()
			if err := w.store.ReleaseLease(releaseCtx, runID, w.cfg.WorkerID); err != nil {
				w.cfg.Logf("release lease %s: %v", runID, err)
			}
		}
	}()

	st, err := w.store.LoadState(runCtx, runID)
	if err != nil {
		w.cfg.Logf("load run %s: %v", runID, err)
		return
	}
	if st.Terminal() {
		return
	}

	// Renew the lease for as long as this run is being worked on. Losing it
	// cancels runCtx, so this worker stops immediately rather than racing the
	// worker that took over.
	go w.heartbeat(runCtx, runID, cancel)

	graphRaw, err := w.store.GetAgent(runCtx, st.AgentName, st.AgentVersion)
	if err != nil {
		w.failRun(runCtx, st, fmt.Sprintf("load agent %s v%d: %v", st.AgentName, st.AgentVersion, err))
		return
	}
	graph, err := agent.Parse(graphRaw)
	if err != nil {
		w.failRun(runCtx, st, fmt.Sprintf("parse agent graph: %v", err))
		return
	}

	claimedAt := w.store.Now()
	if st.CreatedAt.Before(claimedAt) {
		w.observe(func(m *metrics.Metrics) {
			m.QueueWait.Observe(claimedAt.Sub(st.CreatedAt).Seconds())
		})
	}

	resumed := st.StartedAt != nil
	w.observe(func(m *metrics.Metrics) {
		m.RunsStarted.WithLabelValues(fmt.Sprintf("%t", resumed)).Inc()
	})
	if err := w.store.Append(runCtx, st, store.PendingEvent{
		Type: journal.TypeRunStarted,
		Payload: journal.RunStartedPayload{
			Versioned: journal.Versioned{V: journal.CurrentPayloadVersion},
			WorkerID:  w.cfg.WorkerID,
			Resumed:   resumed,
		},
	}); err != nil {
		if errors.Is(err, store.ErrSeqConflict) {
			// Another worker owns this run now. Stop touching it.
			w.cfg.Logf("run %s: lost ownership at start", runID)
			w.observe(func(m *metrics.Metrics) { m.SeqConflicts.Inc() })
			return
		}
		w.cfg.Logf("run %s: append RunStarted: %v", runID, err)
		return
	}

	outcome := w.interpretFrom(runCtx, st, graph, claimedAt)
	if st.Terminal() {
		w.observe(func(m *metrics.Metrics) {
			m.RunsFinished.WithLabelValues(string(st.Status)).Inc()
		})
	}
	if outcome == outcomeParked {
		// Already requeued with a future available_at; the lease went back with
		// it, so do not release it again.
		released = true
	}
}

// heartbeat renews the lease every HeartbeatEvery and cancels the run if
// ownership is lost.
func (w *Worker) heartbeat(ctx context.Context, runID uuid.UUID, cancelRun context.CancelFunc) {
	ticker := time.NewTicker(w.cfg.HeartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := w.store.Heartbeat(ctx, runID, w.cfg.WorkerID, w.cfg.LeaseFor)
			if err != nil {
				w.cfg.Logf("heartbeat %s: %v", runID, err)
				continue
			}
			if !ok {
				// Zero rows updated: someone else owns this run. Stop now --
				// continuing would mean two workers executing one run, and only
				// the seq guard would catch it, after the effect had happened.
				w.cfg.Logf("run %s: lease lost, stopping", runID)
				cancelRun()
				return
			}
		}
	}
}

// backoff returns the delay before a step's next attempt: exponential, with
// jitter so a fleet retrying one provider outage does not synchronise into a
// thundering herd.
//
// math/rand/v2's top-level functions are safe for concurrent use. A shared
// *rand.Rand is not, and every goroutine in the pool calls this -- the race
// detector caught exactly that.
func (w *Worker) backoff(attempt int) time.Duration {
	d := w.cfg.BaseBackoff << attempt
	if d > w.cfg.MaxBackoff {
		d = w.cfg.MaxBackoff
	}
	half := int64(d / 2)
	return d/2 + time.Duration(rand.N(half+1))
}

// failRun writes a terminal RunFailed for a run that cannot continue.
//
// It takes the already-folded state rather than reloading, so the append uses
// the seq this worker believes is next -- and loses the race, harmlessly, if
// another worker has moved the run on.
func (w *Worker) failRun(ctx context.Context, st *journal.RunState, reason string) {
	if st.Terminal() {
		return
	}
	err := w.store.Append(ctx, st, store.PendingEvent{
		Type: journal.TypeRunFailed,
		Payload: journal.RunFailedPayload{
			Versioned: journal.Versioned{V: journal.CurrentPayloadVersion},
			Error:     reason,
		},
	})
	if err != nil && !errors.Is(err, store.ErrSeqConflict) {
		w.cfg.Logf("run %s: append RunFailed: %v", st.RunID, err)
	}
}
