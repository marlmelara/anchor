package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/marlmelara/anchor/internal/journal"
)

// PendingEvent is an event a caller wants to append. Seq and created_at are
// assigned by Append, never by the caller: seq comes from the folded state and
// the timestamp from the Store's clock.
type PendingEvent struct {
	Type    journal.EventType
	Payload any
}

// Append writes events for a run and updates its projections in one
// transaction, then folds them into st.
//
// This is the only write path into run_events. Its contract:
//
//   - seq is taken from st.NextSeq, so two workers holding the same run compute
//     the same seq and exactly one of them commits. The loser gets
//     ErrSeqConflict and must release the run -- it no longer owns it.
//   - the candidate events are folded into a clone first, so an event that
//     would corrupt the run is rejected before it reaches the database.
//   - st is mutated only after the transaction commits. If Append returns an
//     error, st is exactly as it was.
func (s *Store) Append(ctx context.Context, st *journal.RunState, pending ...PendingEvent) error {
	if len(pending) == 0 {
		return nil
	}
	var next *journal.RunState
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		next, err = s.AppendTx(ctx, tx, st, pending...)
		return err
	})
	if err != nil {
		return err
	}
	// st is advanced only after the transaction commits, so a caller that gets
	// an error still holds exactly the state it had before.
	*st = *next
	return nil
}

// AppendTx is Append inside a caller-supplied transaction, for operations that
// must be atomic with the append -- enqueueing a submitted run, releasing a
// lease as the run completes. It returns the advanced state rather than
// mutating st, because the caller cannot know the transaction committed until
// it returns.
func (s *Store) AppendTx(ctx context.Context, tx pgx.Tx, st *journal.RunState, pending ...PendingEvent) (*journal.RunState, error) {
	if len(pending) == 0 {
		return st.Clone(), nil
	}

	now := s.Now()
	next := st.Clone()

	events := make([]journal.Event, len(pending))
	for i, p := range pending {
		raw, err := journal.MarshalPayload(p.Payload)
		if err != nil {
			return nil, fmt.Errorf("append %s: %w", p.Type, err)
		}
		events[i] = journal.Event{
			RunID:     runIDFor(next, st),
			Seq:       next.NextSeq,
			Type:      p.Type,
			Payload:   raw,
			CreatedAt: now,
		}
		// Fold as we go: each event's validity depends on the ones before it.
		if err := next.Apply(events[i]); err != nil {
			return nil, fmt.Errorf("append %s: %w", p.Type, err)
		}
		events[i].RunID = next.RunID
	}

	touched, err := touchedSteps(events)
	if err != nil {
		return nil, err
	}

	// runs is written first: run_events has a foreign key to it, so the very
	// first append has to create the row it points at.
	if err := upsertRun(ctx, tx, next); err != nil {
		return nil, err
	}
	for _, e := range events {
		_, err := tx.Exec(ctx, `
			INSERT INTO run_events (run_id, seq, type, payload, created_at)
			VALUES ($1, $2, $3, $4, $5)`,
			e.RunID, e.Seq, string(e.Type), []byte(e.Payload), e.CreatedAt)
		if err != nil {
			if isUniqueViolation(err, "run_events_run_seq_unique") {
				return nil, ErrSeqConflict
			}
			return nil, fmt.Errorf("insert run_event seq %d: %w", e.Seq, err)
		}
	}
	if err := upsertSteps(ctx, tx, next, touched); err != nil {
		return nil, err
	}
	return next, nil
}

// runIDFor picks the run id for an event being appended. Every event after seq
// 0 belongs to the run already folded; seq 0 carries the id the caller seeded.
func runIDFor(next, original *journal.RunState) uuid.UUID {
	if next.RunID != uuid.Nil {
		return next.RunID
	}
	return original.RunID
}

// touchedSteps returns the step indexes the given events affect.
//
// Only these rows are upserted. Rewriting every step on every append would make
// a long agent_loop quadratic in its own length, which is exactly the shape of
// run Anchor is built for.
func touchedSteps(events []journal.Event) ([]int, error) {
	seen := map[int]bool{}
	var out []int
	for _, e := range events {
		var probe struct {
			StepIndex *int `json:"step_index"`
		}
		if err := json.Unmarshal(e.Payload, &probe); err != nil {
			return nil, fmt.Errorf("probe step_index on %s: %w", e.Type, err)
		}
		if probe.StepIndex == nil || seen[*probe.StepIndex] {
			continue
		}
		seen[*probe.StepIndex] = true
		out = append(out, *probe.StepIndex)
	}
	return out, nil
}

// LoadEvents reads a run's complete log in seq order.
func (s *Store) LoadEvents(ctx context.Context, runID uuid.UUID) ([]journal.Event, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, run_id, seq, type, payload, created_at
		FROM run_events
		WHERE run_id = $1
		ORDER BY seq`, runID)
	if err != nil {
		return nil, fmt.Errorf("query run_events: %w", err)
	}
	defer rows.Close()

	var out []journal.Event
	for rows.Next() {
		var e journal.Event
		var typ string
		var payload []byte
		if err := rows.Scan(&e.ID, &e.RunID, &e.Seq, &typ, &payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan run_event: %w", err)
		}
		e.Type = journal.EventType(typ)
		e.Payload = json.RawMessage(payload)
		// Postgres returns timestamptz in the session's timezone. The instant is
		// the same either way, but the fold's output is compared byte-for-byte
		// during replay verification, so it has to be canonical: always UTC.
		e.CreatedAt = e.CreatedAt.UTC()
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run_events: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("run %s: %w", runID, ErrNotFound)
	}
	return out, nil
}

// LoadState folds a run from its log. This is what a worker calls when it
// claims a run: state is never read from the projections, only rebuilt from the
// events, so a stale or corrupted projection cannot influence execution.
func (s *Store) LoadState(ctx context.Context, runID uuid.UUID) (*journal.RunState, error) {
	events, err := s.LoadEvents(ctx, runID)
	if err != nil {
		return nil, err
	}
	return journal.Fold(events)
}
