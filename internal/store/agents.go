package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RegisterAgent stores an agent definition and returns its new version number.
//
// Versions are append-only. A run pins the version it was submitted against, so
// replaying a run from last month walks the graph as it was last month, not as
// it is today. This is the reason agents are data rather than code.
func (s *Store) RegisterAgent(ctx context.Context, name string, definition json.RawMessage) (int, error) {
	if name == "" {
		return 0, errors.New("agent name is required")
	}
	if !json.Valid(definition) {
		return 0, errors.New("agent definition is not valid JSON")
	}

	// Two registrations of the same agent can compute the same next version and
	// collide on the primary key. The loser simply recomputes; there is no need
	// for a sequence per agent.
	const attempts = 3
	var lastErr error
	for range attempts {
		var version int
		err := s.pool.QueryRow(ctx, `
			INSERT INTO agents (name, version, definition)
			SELECT $1, COALESCE(MAX(version), 0) + 1, $2 FROM agents WHERE name = $1
			RETURNING version`, name, []byte(definition)).Scan(&version)
		if err == nil {
			return version, nil
		}
		if !isUniqueViolation(err, "") {
			return 0, fmt.Errorf("register agent %q: %w", name, err)
		}
		lastErr = err
	}
	return 0, fmt.Errorf("register agent %q: lost the version race %d times: %w", name, attempts, lastErr)
}

// GetAgent reads one version of an agent definition.
func (s *Store) GetAgent(ctx context.Context, name string, version int) (json.RawMessage, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT definition FROM agents WHERE name = $1 AND version = $2`,
		name, version).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("agent %s v%d: %w", name, version, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get agent %s v%d: %w", name, version, err)
	}
	return json.RawMessage(raw), nil
}

// LatestAgentVersion returns the highest registered version of an agent.
func (s *Store) LatestAgentVersion(ctx context.Context, name string) (int, error) {
	var version int
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM agents WHERE name = $1`, name).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("latest agent version %q: %w", name, err)
	}
	if version == 0 {
		return 0, fmt.Errorf("agent %q: %w", name, ErrNotFound)
	}
	return version, nil
}
