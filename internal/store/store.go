// Package store is Anchor's Postgres layer: the append path, the projections
// derived from it, and the queue and lease tables the worker pool claims from.
//
// The single rule this package exists to enforce is that an event append and
// the projection update it implies happen in one transaction. If they could
// drift, `anchorctl rebuild` would produce a different answer than the live
// tables, and every guarantee above this layer would be a guess.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrSeqConflict is returned when an append loses the race for a run's next
// seq. It is not an error condition in the usual sense: it means another worker
// owns this run now, and the correct response is to release it and move on.
var ErrSeqConflict = errors.New("run event seq conflict: another writer advanced this run")

// ErrNotFound is returned when a run, agent or lease does not exist.
var ErrNotFound = errors.New("not found")

// Store owns a pgx pool.
type Store struct {
	pool *pgxpool.Pool

	// Now supplies event timestamps. It is a field rather than a call to
	// time.Now() so tests can pin the clock, and so a replay can drive the
	// journal from recorded time instead of the wall clock.
	Now func() time.Time
}

// Config describes a Store's connection.
type Config struct {
	DSN string
	// MaxConns bounds the pool. A worker holds one connection for the whole of
	// a claim transaction, so this must exceed the worker pool size or workers
	// will queue on the pool instead of on the database.
	MaxConns int32
}

// Open connects and verifies the connection.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pcfg.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool, Now: func() time.Time { return time.Now().UTC() }}, nil
}

// Pool exposes the underlying pool for packages that need their own queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// isUniqueViolation reports whether err is a Postgres unique-constraint
// violation on the named constraint.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// 23505 is unique_violation.
	return pgErr.Code == "23505" && (constraint == "" || pgErr.ConstraintName == constraint)
}
