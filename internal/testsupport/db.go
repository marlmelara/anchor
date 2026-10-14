// Package testsupport provides the database fixture shared by Anchor's tests.
//
// Go runs each package's test binary in parallel, so every package that touches
// Postgres would otherwise truncate the tables another package was mid-way
// through using. Each package gets its own schema instead: same database, same
// migrations, separate tables, real concurrency still exercised within a
// package.
package testsupport

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/marlmelara/anchor/internal/store"
)

var safeSchema = regexp.MustCompile(`^[a-z][a-z0-9_]{0,40}$`)

// Store returns a migrated, empty Store scoped to its own schema.
//
// It skips the test when ANCHOR_TEST_DATABASE_URL is unset, so the suite still
// runs without a database -- the pure-logic packages do not need one.
func Store(t *testing.T, schema string) *store.Store {
	t.Helper()
	if !safeSchema.MatchString(schema) {
		t.Fatalf("schema %q is not a safe identifier", schema)
	}
	dsn := os.Getenv("ANCHOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ANCHOR_TEST_DATABASE_URL is not set; skipping database tests")
	}

	ctx := context.Background()
	s, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 24, Schema: schema})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)

	// CREATE SCHEMA resolves without search_path, so this works even though the
	// schema the pool points at does not exist yet.
	if _, err := s.Pool().Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	if _, err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// TRUNCATE rather than DELETE: run_events carries an append-only trigger,
	// and TRUNCATE does not fire row-level triggers.
	if _, err := s.Pool().Exec(ctx,
		`TRUNCATE queue, leases, steps, run_events, runs, agents RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}
