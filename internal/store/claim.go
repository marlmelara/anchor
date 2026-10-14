package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrNoWork is returned by ClaimNext when the queue has nothing ready. It is a
// normal condition, not a failure: the worker backs off and polls again.
var ErrNoWork = errors.New("no work available")

// ClaimNext takes ownership of one runnable run and returns its id.
//
// The whole claim is one transaction:
//
//	SELECT ... FOR UPDATE SKIP LOCKED   pick a row nobody else has locked
//	DELETE FROM queue                   it is no longer pending
//	INSERT INTO leases                  it now has an owner
//
// All three commit together, so a run can never be out of the queue without an
// owner, or owned while still queued. FOR UPDATE locks the chosen row; SKIP
// LOCKED makes other workers step over rows already locked instead of waiting
// behind them, which is what lets N workers claim N different runs with no
// blocking and no external queue.
func (s *Store) ClaimNext(ctx context.Context, workerID string, leaseFor time.Duration) (uuid.UUID, error) {
	if workerID == "" {
		return uuid.Nil, errors.New("worker id is required")
	}
	now := pgTimestamp(s.Now())
	expires := now.Add(leaseFor)

	var runID uuid.UUID
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			SELECT run_id FROM queue
			WHERE available_at <= $1
			ORDER BY priority, available_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1`, now).Scan(&runID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoWork
		}
		if err != nil {
			return fmt.Errorf("select queued run: %w", err)
		}

		if _, err := tx.Exec(ctx, `DELETE FROM queue WHERE run_id = $1`, runID); err != nil {
			return fmt.Errorf("dequeue %s: %w", runID, err)
		}

		// ON CONFLICT DO UPDATE: a run whose previous owner died leaves a stale
		// lease row behind. Taking it over is an overwrite, not an insert.
		_, err = tx.Exec(ctx, `
			INSERT INTO leases (run_id, worker_id, acquired_at, heartbeat_at, expires_at)
			VALUES ($1, $2, $3, $3, $4)
			ON CONFLICT (run_id) DO UPDATE
			SET worker_id = EXCLUDED.worker_id,
			    acquired_at = EXCLUDED.acquired_at,
			    heartbeat_at = EXCLUDED.heartbeat_at,
			    expires_at = EXCLUDED.expires_at`,
			runID, workerID, now, expires)
		if err != nil {
			return fmt.Errorf("acquire lease on %s: %w", runID, err)
		}
		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return runID, nil
}

// ReleaseLease gives up ownership of a run.
//
// The worker_id check means a worker that already lost its lease to a takeover
// cannot delete the new owner's lease on its way out.
func (s *Store) ReleaseLease(ctx context.Context, runID uuid.UUID, workerID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM leases WHERE run_id = $1 AND worker_id = $2`, runID, workerID)
	if err != nil {
		return fmt.Errorf("release lease on %s: %w", runID, err)
	}
	return nil
}

// Requeue puts a run back on the queue, available at the given time.
//
// Used for two things: a run that lost its seq race and must be picked up again
// by whoever actually owns it, and a retry whose backoff has not elapsed. In
// the retry case the run is parked in the future rather than slept on, so the
// worker goroutine is freed for other runs instead of holding a slot doing
// nothing.
func (s *Store) Requeue(ctx context.Context, runID uuid.UUID, workerID string, availableAt time.Time, priority int) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`DELETE FROM leases WHERE run_id = $1 AND worker_id = $2`, runID, workerID); err != nil {
			return fmt.Errorf("release lease: %w", err)
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO queue (run_id, available_at, priority, enqueued_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (run_id) DO UPDATE SET available_at = EXCLUDED.available_at`,
			runID, pgTimestamp(availableAt), priority, pgTimestamp(s.Now()))
		if err != nil {
			return fmt.Errorf("requeue: %w", err)
		}
		return nil
	})
	return err
}

// Heartbeat renews a lease and reports whether this worker still owns it.
//
// A false return means another worker took the run over while this one was
// slow -- a long garbage-collection pause is enough. The correct response is to
// stop work on that run immediately; continuing would mean two workers
// executing the same run, and only the (run_id, seq) guard would catch it,
// after the effect had already happened.
func (s *Store) Heartbeat(ctx context.Context, runID uuid.UUID, workerID string, leaseFor time.Duration) (bool, error) {
	now := pgTimestamp(s.Now())
	tag, err := s.pool.Exec(ctx, `
		UPDATE leases SET heartbeat_at = $1, expires_at = $2
		WHERE run_id = $3 AND worker_id = $4`,
		now, now.Add(leaseFor), runID, workerID)
	if err != nil {
		return false, fmt.Errorf("heartbeat on %s: %w", runID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// QueueDepth is the number of runs waiting. Exported as a Prometheus gauge in
// M5 and used by the API to decide when to answer 429.
func (s *Store) QueueDepth(ctx context.Context) (int64, error) {
	var depth int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM queue`).Scan(&depth); err != nil {
		return 0, fmt.Errorf("queue depth: %w", err)
	}
	return depth, nil
}
