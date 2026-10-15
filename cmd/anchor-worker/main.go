// Command anchor-worker claims runs from Postgres and executes them.
//
// Several copies run side by side with no coordination between them: the queue
// claim uses SELECT ... FOR UPDATE SKIP LOCKED, so each worker takes a
// different run without blocking on the others.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/marlmelara/anchor/internal/metrics"
	"github.com/marlmelara/anchor/internal/provider"
	"github.com/marlmelara/anchor/internal/ratelimit"
	"github.com/marlmelara/anchor/internal/sandbox"
	"github.com/marlmelara/anchor/internal/store"
	"github.com/marlmelara/anchor/internal/tool"
	"github.com/marlmelara/anchor/internal/worker"
)

func main() {
	dsn := flag.String("dsn", "", "postgres DSN (default: $ANCHOR_DATABASE_URL)")
	poolSize := flag.Int("pool", 4, "runs executed concurrently by this process")
	workerID := flag.String("id", "", "worker id (default: hostname plus a random suffix)")
	leaseFor := flag.Duration("lease", 30*time.Second, "lease duration")
	heartbeat := flag.Duration("heartbeat", 10*time.Second, "lease renewal interval")
	providerName := flag.String("provider", "mock", "model provider: mock or anthropic")
	sandboxKind := flag.String("sandbox", "mock", "tool runner: mock (in-process) or docker")
	allowEgress := flag.Bool("allow-egress", false, "permit network access to tools that declare it")
	mockLatency := flag.Duration("mock-latency", 50*time.Millisecond, "fixed latency for the mock provider")
	chaosTools := flag.Bool("chaos-tools", false, "register the chaos counting tool (test harness only)")
	reapEvery := flag.Duration("reap", 5*time.Second, "how often to requeue runs whose lease expired")
	schema := flag.String("schema", "", "postgres search_path (default: public)")
	redisURL := flag.String("redis", "", "redis URL for rate limits and the circuit breaker (default: $ANCHOR_REDIS_URL; empty disables both)")
	tenantRate := flag.Float64("tenant-rate", 20, "sustained model calls per second per tenant")
	tenantBurst := flag.Float64("tenant-burst", 40, "burst allowance per tenant")
	providerRate := flag.Float64("provider-rate", 100, "sustained model calls per second against the provider")
	providerBurst := flag.Float64("provider-burst", 200, "burst allowance against the provider")
	metricsAddr := flag.String("metrics-addr", "", "serve Prometheus metrics on this address, e.g. :9090")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := options{
		dsn: *dsn, poolSize: *poolSize, workerID: *workerID,
		leaseFor: *leaseFor, heartbeat: *heartbeat, reapEvery: *reapEvery,
		providerName: *providerName, mockLatency: *mockLatency,
		chaosTools: *chaosTools, schema: *schema,
		sandboxKind: *sandboxKind, allowEgress: *allowEgress,
		redisURL: *redisURL, metricsAddr: *metricsAddr,
		tenantRate: *tenantRate, tenantBurst: *tenantBurst,
		providerRate: *providerRate, providerBurst: *providerBurst,
	}
	if err := run(cfg, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// options groups the flags so the signature stays readable as they accumulate.
type options struct {
	dsn           string
	poolSize      int
	workerID      string
	leaseFor      time.Duration
	heartbeat     time.Duration
	reapEvery     time.Duration
	providerName  string
	mockLatency   time.Duration
	chaosTools    bool
	schema        string
	sandboxKind   string
	allowEgress   bool
	redisURL      string
	metricsAddr   string
	tenantRate    float64
	tenantBurst   float64
	providerRate  float64
	providerBurst float64
}

func run(o options, log *slog.Logger) error {
	dsn, poolSize, workerID := o.dsn, o.poolSize, o.workerID
	leaseFor, heartbeat := o.leaseFor, o.heartbeat
	providerName, mockLatency, chaosTools := o.providerName, o.mockLatency, o.chaosTools
	if dsn == "" {
		dsn = os.Getenv("ANCHOR_DATABASE_URL")
	}
	if dsn == "" {
		return errors.New("no database DSN: pass -dsn or set ANCHOR_DATABASE_URL")
	}
	if workerID == "" {
		host, _ := os.Hostname()
		workerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	// Two contexts, and the difference between them is what makes shutdown
	// graceful. claimCtx dies on SIGTERM so goroutines stop taking NEW runs.
	// runCtx stays alive so work already in flight can finish and journal its
	// result rather than being abandoned mid-step.
	claimCtx, stopClaiming := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopClaiming()
	runCtx, stopRuns := context.WithCancel(context.WithoutCancel(claimCtx))
	defer stopRuns()

	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The pool must exceed the worker pool size: each goroutine holds a
	// connection for the whole of its claim transaction.
	s, err := store.Open(connectCtx, store.Config{
		DSN: dsn, MaxConns: int32(poolSize) + 4, Schema: o.schema,
	})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()

	var modelProvider provider.Provider
	switch providerName {
	case "mock":
		m := provider.NewMock("done")
		m.Latency = mockLatency
		modelProvider = m
	case "anthropic":
		// No API key is read here: the SDK resolves credentials itself, so an
		// unset ANTHROPIC_API_KEY does not mean there are none -- a profile
		// written by `ant auth login` works too.
		modelProvider = provider.NewAnthropic(provider.AnthropicConfig{
			Timeout: 2 * time.Minute,
		})
	default:
		return fmt.Errorf("unknown provider %q (want mock or anthropic)", providerName)
	}

	registry := tool.NewRegistry()
	mockRunner := tool.NewMockRunner()
	echoDef, echoFn := tool.Echo("echo")
	_ = registry.Register(echoDef)
	mockRunner.Handle("echo", echoFn)

	if chaosTools {
		if err := registerChaosTool(context.Background(), s, registry, mockRunner); err != nil {
			return fmt.Errorf("register chaos tool: %w", err)
		}
		log.Info("chaos tools registered")
	}

	var inner tool.Runner = mockRunner
	if o.sandboxKind == "docker" {
		box, err := sandbox.NewDocker(sandbox.Config{
			WorkerID:    workerID,
			AllowEgress: o.allowEgress,
			Logf:        func(format string, args ...any) { log.Warn(fmt.Sprintf(format, args...)) },
		})
		if err != nil {
			return fmt.Errorf("start sandbox: %w", err)
		}
		defer func() { _ = box.Close() }()
		if err := box.Ping(context.Background()); err != nil {
			return fmt.Errorf("docker is not reachable: %w", err)
		}

		// Sweep containers an earlier worker left behind. A worker killed
		// mid-tool-call never ran its deferred removal, and each orphan holds
		// memory and a CPU share the host has not reclaimed.
		reaped, err := box.ReapOrphans(context.Background())
		if err != nil {
			log.Warn("reap orphaned containers", "err", err)
		} else if reaped > 0 {
			log.Info("reaped orphaned containers", "count", reaped)
		}
		inner = box
	} else if o.sandboxKind != "mock" {
		return fmt.Errorf("unknown sandbox %q (want mock or docker)", o.sandboxKind)
	}

	// Schema validation wraps whichever runner is in use, so the rule is
	// written once and the sandbox and the mock behave identically.
	runner := tool.NewValidatingRunner(inner)

	// Metrics and shared protections. Both are optional: without Redis the
	// worker runs with no rate limiting and no breaker, which is what the
	// mock-provider benchmarks want so the numbers measure Anchor alone.
	m := metrics.New()
	// Create the zero-valued breaker series up front, so its dashboard panel
	// reads 0 rather than "no data" before the first call.
	m.InitProvider(modelProvider.Name())
	var guards *worker.Guards
	redisAddr := o.redisURL
	if redisAddr == "" {
		redisAddr = os.Getenv("ANCHOR_REDIS_URL")
	}
	if redisAddr != "" {
		opts, err := redis.ParseURL(redisAddr)
		if err != nil {
			return fmt.Errorf("parse redis url: %w", err)
		}
		rdb := redis.NewClient(opts)
		defer func() { _ = rdb.Close() }()
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			pingCancel()
			return fmt.Errorf("redis unreachable at %s: %w", redisAddr, err)
		}
		pingCancel()
		guards = worker.NewGuards(rdb, worker.GuardConfig{
			ProviderName: modelProvider.Name(),
			PerTenant:    ratelimit.BucketConfig{Capacity: o.tenantBurst, RefillPerSecond: o.tenantRate},
			PerProvider:  ratelimit.BucketConfig{Capacity: o.providerBurst, RefillPerSecond: o.providerRate},
		})
		log.Info("rate limits and circuit breaker enabled", "redis", redisAddr)
	} else {
		log.Info("no redis configured: rate limits and circuit breaker are disabled")
	}

	if o.metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry(), promhttp.HandlerOpts{}))
		metricsSrv := &http.Server{Addr: o.metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "err", err)
			}
		}()
		defer func() {
			shutdownCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
			defer c()
			_ = metricsSrv.Shutdown(shutdownCtx)
		}()
		log.Info("serving metrics", "addr", o.metricsAddr)
	}

	w := worker.New(s, modelProvider, registry, runner, worker.Config{
		WorkerID:       workerID,
		PoolSize:       poolSize,
		LeaseFor:       leaseFor,
		HeartbeatEvery: heartbeat,
		ReapEvery:      o.reapEvery,
		Guards:         guards,
		Metrics:        m,
		Logf:           func(format string, args ...any) { log.Info(fmt.Sprintf(format, args...)) },
	})

	log.Info("worker started", "id", workerID, "pool", poolSize, "provider", providerName, "sandbox", o.sandboxKind)

	done := make(chan error, 1)
	go func() { done <- w.Run(claimCtx, runCtx) }()

	<-claimCtx.Done()
	log.Info("SIGTERM received, finishing in-flight runs")

	// In-flight runs get a bounded window to finish or checkpoint. Past it they
	// are abandoned, their leases expire, and another worker resumes them from
	// the journal -- no progress is lost either way.
	select {
	case err := <-done:
		log.Info("drained cleanly")
		return err
	case <-time.After(leaseFor):
		log.Warn("drain timed out; abandoning in-flight runs to lease expiry")
		stopRuns()
		return nil
	}
}

// registerChaosTool wires up the tool the chaos suite measures.
//
// The effect is a row in a real table in a real database, not an in-memory
// counter. That distinction is the entire point: an in-process counter dies
// with the process it lives in, so it could never prove anything about a
// worker that was SIGKILLed.
//
// The tool declares itself idempotent, so it has to BE idempotent. Anchor
// retries an idempotent tool that died in the ambiguous window -- that is the
// whole contract -- so a tool that inserted a fresh row on every call would
// produce two rows for one logical step and look like a duplicate effect when
// the system was behaving exactly as designed.
//
// Two tables, measuring two different things:
//
//   - chaos_effects has a UNIQUE marker. Its row count is the number of
//     EFFECTIVELY-ONCE effects, which is what Anchor actually promises:
//     at-least-once delivery plus an idempotent effect.
//   - chaos_attempts records every execution. Its count may EXCEED the number
//     of runs under chaos, and that is evidence the kills landed mid-effect
//     rather than a failure.
func registerChaosTool(ctx context.Context, s *store.Store, registry *tool.Registry, runner *tool.MockRunner) error {
	if _, err := s.Pool().Exec(ctx, `
		CREATE TABLE IF NOT EXISTS chaos_effects (
			marker     text        PRIMARY KEY,
			created_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create chaos_effects: %w", err)
	}
	if _, err := s.Pool().Exec(ctx, `
		CREATE TABLE IF NOT EXISTS chaos_attempts (
			id         bigserial   PRIMARY KEY,
			marker     text        NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create chaos_attempts: %w", err)
	}

	def := tool.Definition{
		Name:        "chaos_count",
		Description: "records one effect; the chaos suite counts effects and attempts separately",
		// Idempotent, and genuinely so: the INSERT below is a no-op on repeat.
		// Declaring this without implementing it is what produced an
		// intermittent "duplicate effect" that was really the retry contract
		// working correctly.
		Idempotent: true,
	}
	if err := registry.Register(def); err != nil {
		return err
	}

	runner.Handle("chaos_count", func(ctx context.Context, input json.RawMessage) (tool.Result, error) {
		var in struct {
			Marker string `json:"marker"`
		}
		if err := json.Unmarshal(input, &in); err != nil {
			return tool.Result{}, fmt.Errorf("chaos_count input: %w", err)
		}
		if in.Marker == "" {
			return tool.Result{}, errors.New("chaos_count requires a marker")
		}

		// Every execution is recorded, including repeats.
		if _, err := s.Pool().Exec(ctx,
			`INSERT INTO chaos_attempts (marker) VALUES ($1)`, in.Marker); err != nil {
			return tool.Result{}, fmt.Errorf("record attempt: %w", err)
		}
		// The effect itself happens at most once per marker. This is what
		// "idempotent" has to mean in practice: repeating is harmless because
		// the second attempt changes nothing.
		if _, err := s.Pool().Exec(ctx,
			`INSERT INTO chaos_effects (marker) VALUES ($1) ON CONFLICT (marker) DO NOTHING`,
			in.Marker); err != nil {
			return tool.Result{}, fmt.Errorf("record effect: %w", err)
		}

		out, err := json.Marshal(map[string]any{"marker": in.Marker})
		if err != nil {
			return tool.Result{}, err
		}
		return tool.Result{Output: out, ExitCode: 0}, nil
	})
	return nil
}
