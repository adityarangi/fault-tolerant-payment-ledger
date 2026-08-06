package storage

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/migrations"
)

var migrationFS = migrations.FS

// Migration is a single versioned schema change.
type Migration struct {
	Version int
	Name    string
	Up      string
	Down    string
}

const migrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INT PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// LoadMigrations reads the embedded migration files.
func LoadMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationFS, ".")
	if err != nil {
		return nil, fmt.Errorf("storage: read migrations: %w", err)
	}

	byVersion := make(map[int]*Migration)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		// Files are named <version>_<name>.(up|down).sql
		versionStr, rest, ok := strings.Cut(name, "_")
		if !ok {
			return nil, fmt.Errorf("storage: malformed migration name %q", name)
		}
		version, err := strconv.Atoi(versionStr)
		if err != nil {
			return nil, fmt.Errorf("storage: malformed migration version in %q: %w", name, err)
		}
		body, err := migrationFS.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("storage: read %q: %w", name, err)
		}

		m := byVersion[version]
		if m == nil {
			m = &Migration{Version: version}
			byVersion[version] = m
		}
		switch {
		case strings.HasSuffix(rest, ".up.sql"):
			m.Name = strings.TrimSuffix(rest, ".up.sql")
			m.Up = string(body)
		case strings.HasSuffix(rest, ".down.sql"):
			m.Name = strings.TrimSuffix(rest, ".down.sql")
			m.Down = string(body)
		default:
			return nil, fmt.Errorf("storage: migration %q is neither .up.sql nor .down.sql", name)
		}
	}

	out := make([]Migration, 0, len(byVersion))
	for _, m := range byVersion {
		if m.Up == "" {
			return nil, fmt.Errorf("storage: migration %d has no up file", m.Version)
		}
		if m.Down == "" {
			return nil, fmt.Errorf("storage: migration %d has no down file", m.Version)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// MigrateUp applies every migration that has not yet been applied. Each
// migration runs in its own transaction together with its bookkeeping row, so
// a failure can never record a migration that did not fully apply.
func MigrateUp(ctx context.Context, pool *pgxpool.Pool) ([]int, error) {
	list, err := LoadMigrations()
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, migrationsTable); err != nil {
		return nil, fmt.Errorf("storage: create migrations table: %w", err)
	}
	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return nil, err
	}

	var ran []int
	for _, m := range list {
		if applied[m.Version] {
			continue
		}
		err := withTx(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.Up); err != nil {
				return fmt.Errorf("apply %d_%s: %w", m.Version, m.Name, err)
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name)
			return err
		})
		if err != nil {
			return ran, fmt.Errorf("storage: migrate up: %w", err)
		}
		ran = append(ran, m.Version)
	}
	return ran, nil
}

// MigrateDown rolls back the newest `steps` migrations (0 means all).
func MigrateDown(ctx context.Context, pool *pgxpool.Pool, steps int) ([]int, error) {
	list, err := LoadMigrations()
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, migrationsTable); err != nil {
		return nil, fmt.Errorf("storage: create migrations table: %w", err)
	}
	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return nil, err
	}

	var reverted []int
	for i := len(list) - 1; i >= 0; i-- {
		if steps > 0 && len(reverted) >= steps {
			break
		}
		m := list[i]
		if !applied[m.Version] {
			continue
		}
		err := withTx(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.Down); err != nil {
				return fmt.Errorf("revert %d_%s: %w", m.Version, m.Name, err)
			}
			_, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.Version)
			return err
		})
		if err != nil {
			return reverted, fmt.Errorf("storage: migrate down: %w", err)
		}
		reverted = append(reverted, m.Version)
	}
	return reverted, nil
}

// AppliedVersions returns the sorted list of applied migration versions.
func AppliedVersions(ctx context.Context, pool *pgxpool.Pool) ([]int, error) {
	if _, err := pool.Exec(ctx, migrationsTable); err != nil {
		return nil, err
	}
	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, len(applied))
	for v := range applied {
		out = append(out, v)
	}
	sort.Ints(out)
	return out, nil
}

func appliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int]bool, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("storage: read applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

func withTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
