package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/marlmelara/anchor/internal/journal"
)

// RebuildResult reports what a rebuild did.
type RebuildResult struct {
	Runs     int
	Steps    int
	Mismatch []string
}

// RebuildRun drops and recomputes the runs and steps projections for one run
// from run_events alone.
//
// This command is the proof that run_events really is the source of truth. If
// it ever produces a different answer than the live projection, the append path
// and the fold have drifted and one of them is wrong. `anchorctl rebuild
// --verify` reports exactly that difference instead of silently repairing it.
func (s *Store) RebuildRun(ctx context.Context, runID uuid.UUID, verify bool) (*RebuildResult, error) {
	events, err := s.LoadEvents(ctx, runID)
	if err != nil {
		return nil, err
	}
	st, err := journal.Fold(events)
	if err != nil {
		return nil, fmt.Errorf("fold run %s: %w", runID, err)
	}

	res := &RebuildResult{Runs: 1, Steps: len(st.Steps)}

	if verify {
		live, err := s.loadProjectedRun(ctx, runID)
		if err != nil {
			return nil, err
		}
		res.Mismatch = diffRun(live, st)
		return res, nil
	}

	all := make([]int, len(st.Steps))
	for i := range st.Steps {
		all[i] = i
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Delete the projection rows, then rebuild them from the fold. The
		// event log is never touched -- a trigger would reject it anyway.
		if _, err := tx.Exec(ctx, `DELETE FROM steps WHERE run_id = $1`, runID); err != nil {
			return fmt.Errorf("clear steps: %w", err)
		}
		if err := upsertRun(ctx, tx, st); err != nil {
			return err
		}
		return upsertSteps(ctx, tx, st, all)
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// RebuildAll rebuilds every run that has events.
func (s *Store) RebuildAll(ctx context.Context, verify bool) (*RebuildResult, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT run_id FROM run_events ORDER BY run_id`)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan run_id: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}

	total := &RebuildResult{}
	for _, id := range ids {
		r, err := s.RebuildRun(ctx, id, verify)
		if err != nil {
			return total, fmt.Errorf("run %s: %w", id, err)
		}
		total.Runs += r.Runs
		total.Steps += r.Steps
		total.Mismatch = append(total.Mismatch, r.Mismatch...)
	}
	return total, nil
}

// projectedRun is the runs row as it currently stands, for verification.
type projectedRun struct {
	Status      string
	TokensUsed  int64
	CentsUsed   int64
	Error       *string
	StepCount   int
	StepStatus  map[int]string
	StepAttempt map[int]int
	StepTokens  map[int]int64
}

func (s *Store) loadProjectedRun(ctx context.Context, runID uuid.UUID) (*projectedRun, error) {
	p := &projectedRun{
		StepStatus:  map[int]string{},
		StepAttempt: map[int]int{},
		StepTokens:  map[int]int64{},
	}
	err := s.pool.QueryRow(ctx, `
		SELECT status, tokens_used, cents_used, error
		FROM runs WHERE id = $1`, runID,
	).Scan(&p.Status, &p.TokensUsed, &p.CentsUsed, &p.Error)
	if err != nil {
		return nil, fmt.Errorf("read projected run %s: %w", runID, err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT step_index, status, attempt, tokens
		FROM steps WHERE run_id = $1 ORDER BY step_index`, runID)
	if err != nil {
		return nil, fmt.Errorf("read projected steps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var idx, attempt int
		var status string
		var tokens int64
		if err := rows.Scan(&idx, &status, &attempt, &tokens); err != nil {
			return nil, fmt.Errorf("scan projected step: %w", err)
		}
		p.StepStatus[idx] = status
		p.StepAttempt[idx] = attempt
		p.StepTokens[idx] = tokens
		p.StepCount++
	}
	return p, rows.Err()
}

// diffRun compares the live projection against the fold and returns a
// human-readable list of every disagreement.
func diffRun(live *projectedRun, st *journal.RunState) []string {
	var out []string
	add := func(format string, args ...any) {
		out = append(out, fmt.Sprintf(format, args...))
	}

	if live.Status != string(st.Status) {
		add("run status: projection=%q fold=%q", live.Status, st.Status)
	}
	if live.TokensUsed != st.TokensUsed {
		add("run tokens_used: projection=%d fold=%d", live.TokensUsed, st.TokensUsed)
	}
	if live.CentsUsed != st.CentsUsed {
		add("run cents_used: projection=%d fold=%d", live.CentsUsed, st.CentsUsed)
	}
	liveErr := ""
	if live.Error != nil {
		liveErr = *live.Error
	}
	if liveErr != st.Error {
		add("run error: projection=%q fold=%q", liveErr, st.Error)
	}
	if live.StepCount != len(st.Steps) {
		add("step count: projection=%d fold=%d", live.StepCount, len(st.Steps))
	}
	for _, s := range st.Steps {
		if got, ok := live.StepStatus[s.StepIndex]; !ok {
			add("step %d: missing from projection", s.StepIndex)
			continue
		} else if got != string(s.Status) {
			add("step %d status: projection=%q fold=%q", s.StepIndex, got, s.Status)
		}
		if got := live.StepAttempt[s.StepIndex]; got != s.Attempt {
			add("step %d attempt: projection=%d fold=%d", s.StepIndex, got, s.Attempt)
		}
		if got := live.StepTokens[s.StepIndex]; got != s.Tokens {
			add("step %d tokens: projection=%d fold=%d", s.StepIndex, got, s.Tokens)
		}
	}
	return out
}
