package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/marlmelara/anchor/internal/idem"
	"github.com/marlmelara/anchor/internal/journal"
)

// Reservation is the outcome of trying to claim a step before running it.
type Reservation struct {
	Key string
	// AlreadyRan is true when this exact step was reserved by an earlier
	// attempt -- possibly by a worker that has since died. The effect may
	// already have happened, so the caller must not execute it again.
	AlreadyRan bool
}

// ReserveStep inserts the step's idempotency key and reports whether it was
// already there.
//
// This is the guard that makes at-least-once delivery safe. Before performing
// any side effect the worker reserves the key; the unique index on
// steps.idempotency_key means only the first reservation inserts a row. A
// conflict means this step already ran, so the recorded result is read from the
// journal instead of the effect happening a second time.
//
// Note what this does NOT do: it cannot tell you whether the effect *completed*,
// only that it was *started*. A worker that died between reserving and
// journaling leaves a reservation with no result -- the ambiguous window. Who
// resolves it is decided by the tool's idempotent flag, not here.
func (s *Store) ReserveStep(
	ctx context.Context,
	tx pgx.Tx,
	runID uuid.UUID,
	stepIndex int,
	kind journal.StepKind,
	name string,
	input json.RawMessage,
) (Reservation, error) {
	key, err := idem.Key(runID, stepIndex, input)
	if err != nil {
		return Reservation{}, fmt.Errorf("derive idempotency key: %w", err)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO steps (run_id, step_index, kind, name, idempotency_key, status, attempt)
		VALUES ($1, $2, $3, $4, $5, 'scheduled', 0)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		runID, stepIndex, string(kind), name, key)
	if err != nil {
		return Reservation{}, fmt.Errorf("reserve step %d: %w", stepIndex, err)
	}

	// Zero rows affected means the key was already present: this step ran, or
	// at least started, before.
	return Reservation{Key: key, AlreadyRan: tag.RowsAffected() == 0}, nil
}

// StepRecord is a steps row.
type StepRecord struct {
	RunID          uuid.UUID
	StepIndex      int
	Kind           string
	Name           string
	IdempotencyKey string
	Status         string
	Attempt        int
}

// StepByKey looks up a step by its idempotency key, for recovering what a
// previous attempt did.
func (s *Store) StepByKey(ctx context.Context, key string) (*StepRecord, error) {
	var r StepRecord
	err := s.pool.QueryRow(ctx, `
		SELECT run_id, step_index, kind, name, idempotency_key, status, attempt
		FROM steps WHERE idempotency_key = $1`, key,
	).Scan(&r.RunID, &r.StepIndex, &r.Kind, &r.Name, &r.IdempotencyKey, &r.Status, &r.Attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("step key %s: %w", key, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("lookup step by key: %w", err)
	}
	return &r, nil
}
