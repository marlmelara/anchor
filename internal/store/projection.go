package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/marlmelara/anchor/internal/journal"
)

// upsertRun writes the runs projection for st.
//
// Only fields the fold can change are updated. tenant_id, agent_name,
// agent_version, input, the budgets, created_at and replay_of are written once
// at submission and are immutable thereafter -- listing them in the DO UPDATE
// clause would let a bug quietly rewrite a run's identity.
func upsertRun(ctx context.Context, tx pgx.Tx, st *journal.RunState) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO runs (
			id, tenant_id, agent_name, agent_version, status, input,
			budget_tokens, budget_cents, tokens_used, cents_used,
			created_at, started_at, finished_at, terminal_output, error, replay_of
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16
		)
		ON CONFLICT (id) DO UPDATE SET
			status          = EXCLUDED.status,
			tokens_used     = EXCLUDED.tokens_used,
			cents_used      = EXCLUDED.cents_used,
			started_at      = EXCLUDED.started_at,
			finished_at     = EXCLUDED.finished_at,
			terminal_output = EXCLUDED.terminal_output,
			error           = EXCLUDED.error`,
		st.RunID, st.TenantID, st.AgentName, st.AgentVersion, string(st.Status), jsonOrEmpty(st.Input),
		st.BudgetTokens, st.BudgetCents, st.TokensUsed, st.CentsUsed,
		st.CreatedAt, st.StartedAt, st.FinishedAt, jsonOrNil(st.TerminalOutput), textOrNil(st.Error), st.ReplayOf,
	)
	if err != nil {
		return fmt.Errorf("upsert run %s: %w", st.RunID, err)
	}
	return nil
}

// upsertSteps writes the steps projection for the given indexes.
func upsertSteps(ctx context.Context, tx pgx.Tx, st *journal.RunState, indexes []int) error {
	if len(indexes) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, i := range indexes {
		s, ok := st.Step(i)
		if !ok {
			return fmt.Errorf("upsert step %d: not present in folded state", i)
		}
		batch.Queue(`
			INSERT INTO steps (
				run_id, step_index, kind, name, node_id, idempotency_key,
				status, attempt, started_at, finished_at, tokens, cents, error
			) VALUES (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9, $10, $11, $12, $13
			)
			ON CONFLICT (run_id, step_index) DO UPDATE SET
				status      = EXCLUDED.status,
				attempt     = EXCLUDED.attempt,
				started_at  = EXCLUDED.started_at,
				finished_at = EXCLUDED.finished_at,
				tokens      = EXCLUDED.tokens,
				cents       = EXCLUDED.cents,
				error       = EXCLUDED.error`,
			st.RunID, s.StepIndex, string(s.Kind), s.Name, s.NodeID, s.IdempotencyKey,
			string(s.Status), s.Attempt, s.StartedAt, s.FinishedAt, s.Tokens, s.Cents, textOrNil(s.Error),
		)
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	for range indexes {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("upsert steps: %w", err)
		}
	}
	return nil
}

// jsonOrEmpty renders a nil payload as an empty JSON object, for NOT NULL
// jsonb columns.
func jsonOrEmpty(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte(`{}`)
	}
	return raw
}

// jsonOrNil renders an absent payload as SQL NULL.
func jsonOrNil(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

// textOrNil renders an empty string as SQL NULL, so "no error" is absent rather
// than an empty string a query has to know to filter out.
func textOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
