package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/marlmelara/anchor/migrations"
)

// migrationLockID is an arbitrary but fixed key for a Postgres session advisory
// lock. Every process that migrates takes it first, so a rolling deploy of ten
// workers applies each migration exactly once instead of ten times.
const migrationLockID int64 = 0x414e4348 // "ANCH"

type migration struct {
	version int64
	name    string
	up      string
}

// Migrate applies every migration not yet recorded in schema_migrations.
func (s *Store) Migrate(ctx context.Context) (applied []string, err error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	// The advisory lock is held on this session for the duration and released
	// explicitly; it is not transaction-scoped because each migration runs in
	// its own transaction below.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		if _, uerr := conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockID); uerr != nil && err == nil {
			err = fmt.Errorf("release migration lock: %w", uerr)
		}
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    bigint      PRIMARY KEY,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	done := map[int64]bool{}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var vsn int64
		if err := rows.Scan(&vsn); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan version: %w", err)
		}
		done[vsn] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}

	all, err := loadMigrations()
	if err != nil {
		return nil, err
	}

	for _, m := range all {
		if done[m.version] {
			continue
		}
		// Each migration and its bookkeeping row commit together, so a crash
		// mid-deploy can never record a migration that did not fully apply.
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.up); err != nil {
				return fmt.Errorf("apply %d_%s: %w", m.version, m.name, err)
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
				m.version, m.name)
			return err
		})
		if err != nil {
			return applied, err
		}
		applied = append(applied, fmt.Sprintf("%04d_%s", m.version, m.name))
	}
	return applied, nil
}

// loadMigrations reads and orders the embedded up-migrations.
func loadMigrations() ([]migration, error) {
	entries, err := fs.Glob(migrations.FS, "*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("glob migrations: %w", err)
	}
	out := make([]migration, 0, len(entries))
	for _, name := range entries {
		// Expected shape: 0001_init.up.sql
		base := strings.TrimSuffix(name, ".up.sql")
		num, rest, ok := strings.Cut(base, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q is not named <version>_<name>.up.sql", name)
		}
		vsn, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("migration %q has a non-numeric version: %w", name, err)
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", name, err)
		}
		out = append(out, migration{version: vsn, name: rest, up: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}
