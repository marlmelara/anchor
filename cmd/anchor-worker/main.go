// Command anchor-worker claims runs from Postgres and executes them.
//
// Several copies run side by side with no coordination between them: the queue
// claim uses SELECT ... FOR UPDATE SKIP LOCKED, so each worker takes a
// different run without blocking on the others.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marlmelara/anchor/internal/provider"
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
	providerName := flag.String("provider", "mock", "model provider: mock (real providers arrive in M4)")
	mockLatency := flag.Duration("mock-latency", 50*time.Millisecond, "fixed latency for the mock provider")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(*dsn, *poolSize, *workerID, *leaseFor, *heartbeat, *providerName, *mockLatency, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(
	dsn string, poolSize int, workerID string,
	leaseFor, heartbeat time.Duration,
	providerName string, mockLatency time.Duration,
	log *slog.Logger,
) error {
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
	s, err := store.Open(connectCtx, store.Config{DSN: dsn, MaxConns: int32(poolSize) + 4})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()

	if providerName != "mock" {
		return fmt.Errorf("provider %q is not available yet; real adapters arrive in M4", providerName)
	}
	mock := provider.NewMock("done")
	mock.Latency = mockLatency

	registry := tool.NewRegistry()
	runner := tool.NewMockRunner()
	echoDef, echoFn := tool.Echo("echo")
	_ = registry.Register(echoDef)
	runner.Handle("echo", echoFn)

	w := worker.New(s, mock, registry, runner, worker.Config{
		WorkerID:       workerID,
		PoolSize:       poolSize,
		LeaseFor:       leaseFor,
		HeartbeatEvery: heartbeat,
		Logf:           func(format string, args ...any) { log.Info(fmt.Sprintf(format, args...)) },
	})

	log.Info("worker started", "id", workerID, "pool", poolSize, "provider", providerName)

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
