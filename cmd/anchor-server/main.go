// Command anchor-server is Anchor's HTTP API.
//
// It is stateless: every request reads or writes Postgres and returns. Running
// several copies behind a load balancer needs no coordination between them,
// because they share no state that is not in the database.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marlmelara/anchor/internal/api"
	"github.com/marlmelara/anchor/internal/metrics"
	"github.com/marlmelara/anchor/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dsn := flag.String("dsn", "", "postgres DSN (default: $ANCHOR_DATABASE_URL)")
	highWater := flag.Int64("queue-high-water", 10000, "queue depth above which submits get 429")
	migrate := flag.Bool("migrate", true, "apply pending migrations on start")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(*addr, *dsn, *highWater, *migrate, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(addr, dsn string, highWater int64, migrate bool, log *slog.Logger) error {
	if dsn == "" {
		dsn = os.Getenv("ANCHOR_DATABASE_URL")
	}
	if dsn == "" {
		return errors.New("no database DSN: pass -dsn or set ANCHOR_DATABASE_URL")
	}

	// SIGTERM cancels this, which starts the graceful shutdown below.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	s, err := store.Open(connectCtx, store.Config{DSN: dsn, MaxConns: 16})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()

	if migrate {
		applied, err := s.Migrate(ctx)
		if err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		for _, name := range applied {
			log.Info("applied migration", "name", name)
		}
	}

	srv := &http.Server{
		Addr: addr,
		Handler: api.New(s, api.Config{
			QueueHighWaterMark: highWater,
			APIKeys:            parseAPIKeys(os.Getenv("ANCHOR_API_KEYS")),
			Logger:             log,
			Metrics:            metrics.New(),
		}).Routes(),
		ReadHeaderTimeout: api.ReadHeaderTimeout,
		ReadTimeout:       api.ReadTimeout,
		WriteTimeout:      api.WriteTimeout,
		IdleTimeout:       api.IdleTimeout,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
	}

	// Stop accepting, let in-flight requests finish. Requests are short here
	// because the server never waits on a worker -- it only touches Postgres.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// parseAPIKeys reads "key1:tenant1,key2:tenant2". Empty means run open, which
// is only acceptable locally.
func parseAPIKeys(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	out := map[string]string{}
	for _, pair := range splitAndTrim(raw, ',') {
		key, tenant, ok := cut(pair, ':')
		if !ok || key == "" || tenant == "" {
			continue
		}
		out[key] = tenant
	}
	return out
}

func splitAndTrim(s string, sep rune) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == sep {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		if r == ' ' {
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func cut(s string, sep rune) (before, after string, found bool) {
	for i, r := range s {
		if r == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}
