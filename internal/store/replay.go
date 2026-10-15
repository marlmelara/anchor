package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/marlmelara/anchor/internal/journal"
)

// SubmitReplay queues a replay of an existing run.
//
// A replay is a normal run with replay_of set. It pins the SAME agent version
// as the original -- replaying last month's run against today's graph would
// compare two different programs, and every difference would be reported as a
// determinism failure when it is really a deployment.
func (s *Store) SubmitReplay(ctx context.Context, originalID uuid.UUID) (*journal.RunState, error) {
	original, err := s.LoadState(ctx, originalID)
	if err != nil {
		return nil, fmt.Errorf("load run %s: %w", originalID, err)
	}
	if !original.Terminal() {
		return nil, fmt.Errorf("run %s is still %s; only a finished run can be replayed",
			originalID, original.Status)
	}
	if original.ReplayOf != nil {
		return nil, fmt.Errorf("run %s is itself a replay; replay the original instead", originalID)
	}

	return s.SubmitRun(ctx, SubmitRequest{
		TenantID:     original.TenantID,
		AgentName:    original.AgentName,
		AgentVersion: original.AgentVersion,
		Input:        original.Input,
		// No budgets: a replay spends nothing, and a budget check against
		// recorded token counts could refuse a replay of a run that really
		// happened, which would be nonsense.
		ReplayOf: &originalID,
		// Replays go behind live work. A fidelity sweep of 200 runs must not
		// delay real traffic.
		Priority: 100,
	})
}

// ReplayableRuns lists completed runs eligible for a fidelity sweep.
//
// Only completed runs: a failed or cancelled run has a legitimate reason its
// replay might differ, and including them would make the fidelity number
// describe something other than determinism.
func (s *Store) ReplayableRuns(ctx context.Context, tenantID string, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM runs
		WHERE status = 'completed' AND replay_of IS NULL
		  AND ($1 = '' OR tenant_id = $1)
		ORDER BY created_at DESC
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list replayable runs: %w", err)
	}
	defer rows.Close()

	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan run id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ReplaysOf returns the replay runs made of one original.
func (s *Store) ReplaysOf(ctx context.Context, originalID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id FROM runs WHERE replay_of = $1 ORDER BY created_at`, originalID)
	if err != nil {
		return nil, fmt.Errorf("list replays of %s: %w", originalID, err)
	}
	defer rows.Close()

	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan replay id: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// RunInput returns a run's original input, for building a replay.
func (s *Store) RunInput(ctx context.Context, runID uuid.UUID) (json.RawMessage, error) {
	st, err := s.LoadState(ctx, runID)
	if err != nil {
		return nil, err
	}
	if len(st.Input) == 0 {
		return json.RawMessage(`{}`), nil
	}
	return st.Input, nil
}

// ErrNoReplay is returned when a replay was expected but none exists.
var ErrNoReplay = errors.New("no replay found for this run")
