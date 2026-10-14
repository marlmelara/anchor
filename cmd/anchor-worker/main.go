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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marlmelara/anchor/internal/provider"
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
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := options{
		dsn: *dsn, poolSize: *poolSize, workerID: *workerID,
		leaseFor: *leaseFor, heartbeat: *heartbeat, reapEvery: *reapEvery,
		providerName: *providerName, mockLatency: *mockLatency,
		chaosTools: *chaosTools, schema: *schema,
		sandboxKind: *sandboxKind, allowEgress: *allowEgress,
	}
	if err := run(cfg, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// options groups the flags so the signature stays readable as they accumulate.
type options struct {
	dsn          string
	poolSize     int
	workerID     string
	leaseFor     time.Duration
	heartbeat    time.Duration
	reapEvery    time.Duration
	providerName string
	mockLatency  time.Duration
	chaosTools   bool
	schema       string
	sandboxKind  string
	allowEgress  bool
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

	w := worker.New(s, modelProvider, registry, runner, worker.Config{
		WorkerID:       workerID,
		PoolSize:       poolSize,
		LeaseFor:       leaseFor,
		HeartbeatEvery: heartbeat,
		ReapEvery:      o.reapEvery,
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
// worker that was SIGKILLed. A committed row survives, and counting rows
// afterwards is a direct measurement of how many times the effect actually
// happened.
func registerChaosTool(ctx context.Context, s *store.Store, registry *tool.Registry, runner *tool.MockRunner) error {
	if _, err := s.Pool().Exec(ctx, `
		CREATE TABLE IF NOT EXISTS chaos_effects (
			id         bigserial   PRIMARY KEY,
			marker     text        NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create chaos_effects: %w", err)
	}

	def := tool.Definition{
		Name:        "chaos_count",
		Description: "records one effect row; the chaos suite counts them",
		// Idempotent: repeating this is NOT harmless -- that is what makes it a
		// useful probe. If Anchor ever lets a step run twice, there will be two
		// rows and the test will say so.
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
		var id int64
		if err := s.Pool().QueryRow(ctx,
			`INSERT INTO chaos_effects (marker) VALUES ($1) RETURNING id`, in.Marker,
		).Scan(&id); err != nil {
			return tool.Result{}, fmt.Errorf("record effect: %w", err)
		}
		out, err := json.Marshal(map[string]any{"effect_id": id, "marker": in.Marker})
		if err != nil {
			return tool.Result{}, err
		}
		return tool.Result{Output: out, ExitCode: 0}, nil
	})
	return nil
}
