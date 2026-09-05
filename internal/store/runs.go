package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/marlmelara/anchor/internal/journal"
)

// SubmitRequest is a new run.
type SubmitRequest struct {
	TenantID     string
	AgentName    string
	AgentVersion int
	Input        json.RawMessage
	BudgetTokens int64
	BudgetCents  int64
	Priority     int
	// ReplayOf, when set, marks this run as a reproduction of another run. The
	// worker executes it against a ReplaySource instead of live providers.
	ReplayOf *uuid.UUID
}

// SubmitRun writes RunSubmitted and enqueues the run in one transaction.
//
// Atomicity matters here: a run in the log but not the queue would never be
// picked up, and a run in the queue but not the log would be claimed by a
// worker that then found nothing to fold.
func (s *Store) SubmitRun(ctx context.Context, req SubmitRequest) (*journal.RunState, error) {
	if req.TenantID == "" {
		return nil, errors.New("tenant_id is required")
	}
	if req.AgentName == "" {
		return nil, errors.New("agent_name is required")
	}
	if len(req.Input) == 0 {
		req.Input = json.RawMessage(`{}`)
	}
	if !json.Valid(req.Input) {
		return nil, errors.New("input is not valid JSON")
	}

	st := &journal.RunState{RunID: uuid.New()}
	pending := PendingEvent{
		Type: journal.TypeRunSubmitted,
		Payload: journal.RunSubmittedPayload{
			Versioned:    journal.Versioned{V: journal.CurrentPayloadVersion},
			TenantID:     req.TenantID,
			AgentName:    req.AgentName,
			AgentVersion: req.AgentVersion,
			Input:        req.Input,
			BudgetTokens: req.BudgetTokens,
			BudgetCents:  req.BudgetCents,
			ReplayOf:     req.ReplayOf,
		},
	}

	var next *journal.RunState
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		next, err = s.AppendTx(ctx, tx, st, pending)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO queue (run_id, available_at, priority, enqueued_at)
			VALUES ($1, $2, $3, $2)`,
			next.RunID, s.Now(), req.Priority)
		if err != nil {
			return fmt.Errorf("enqueue run %s: %w", next.RunID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return next, nil
}

// RunSummary is a runs row, for list endpoints and the trace viewer.
type RunSummary struct {
	ID           uuid.UUID  `json:"id"`
	TenantID     string     `json:"tenant_id"`
	AgentName    string     `json:"agent_name"`
	AgentVersion int        `json:"agent_version"`
	Status       string     `json:"status"`
	TokensUsed   int64      `json:"tokens_used"`
	CentsUsed    int64      `json:"cents_used"`
	CreatedAt    time.Time  `json:"created_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	Error        *string    `json:"error,omitempty"`
	ReplayOf     *uuid.UUID `json:"replay_of,omitempty"`
}

// ListRuns reads the runs projection, newest first. This is the one read path
// that uses the projection rather than the fold: it is a listing, it tolerates
// being a moment stale, and folding every run to render a list would be absurd.
func (s *Store) ListRuns(ctx context.Context, tenantID string, limit int) ([]RunSummary, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, agent_name, agent_version, status,
		       tokens_used, cents_used, created_at, started_at, finished_at,
		       error, replay_of
		FROM runs
		WHERE ($1 = '' OR tenant_id = $1)
		ORDER BY created_at DESC
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()

	var out []RunSummary
	for rows.Next() {
		var r RunSummary
		if err := rows.Scan(&r.ID, &r.TenantID, &r.AgentName, &r.AgentVersion, &r.Status,
			&r.TokensUsed, &r.CentsUsed, &r.CreatedAt, &r.StartedAt, &r.FinishedAt,
			&r.Error, &r.ReplayOf); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
