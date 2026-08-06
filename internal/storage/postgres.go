// Package storage owns the PostgreSQL connection pool, schema migrations and
// the transaction helper that makes serialization/deadlock retries uniform.
package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/observability"
)

// DB wraps a pgx pool with the project's transaction conventions.
type DB struct {
	pool       *pgxpool.Pool
	metrics    *observability.Metrics
	maxRetries int
}

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx, letting repository
// methods run inside or outside an explicit transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Open connects to PostgreSQL and verifies the connection.
func Open(ctx context.Context, cfg *config.Config, metrics *observability.Metrics) (*DB, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("storage: parse database url: %w", err)
	}
	poolCfg.MaxConns = cfg.DBMaxConns
	poolCfg.MinConns = cfg.DBMinConns
	poolCfg.MaxConnLifetime = time.Hour
	poolCfg.HealthCheckPeriod = 30 * time.Second

	// Bounding lock and statement time is what turns a potential indefinite
	// deadlock into a retryable error (INVARIANT 8).
	if poolCfg.ConnConfig.RuntimeParams == nil {
		poolCfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	poolCfg.ConnConfig.RuntimeParams["lock_timeout"] = msString(cfg.DBLockTimeout)
	poolCfg.ConnConfig.RuntimeParams["statement_timeout"] = msString(cfg.DBStatementTime)
	poolCfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = msString(30 * time.Second)
	poolCfg.ConnConfig.RuntimeParams["application_name"] = cfg.ServiceName

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("storage: create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("storage: ping: %w", err)
	}

	return &DB{pool: pool, metrics: metrics, maxRetries: cfg.DBMaxTxRetries}, nil
}

// Pool exposes the underlying pool for read-only queries.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Close releases all connections.
func (db *DB) Close() { db.pool.Close() }

// Ping checks liveness of the database.
func (db *DB) Ping(ctx context.Context) error { return db.pool.Ping(ctx) }

// InTx runs fn inside a READ COMMITTED transaction, retrying on deadlock and
// serialization failures with exponential backoff plus jitter.
//
// fn must be idempotent with respect to its own in-memory state: it may be
// invoked more than once. Anything it wrote in a failed attempt is rolled back
// by PostgreSQL before the retry, so no partial state can survive
// (INVARIANT 5).
func (db *DB) InTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	var lastErr error
	for attempt := 0; attempt < db.maxRetries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoff(attempt)); err != nil {
				return err
			}
		}

		err := db.runOnce(ctx, fn)
		if err == nil {
			db.metrics.PostgresTxTotal.WithLabelValues("commit").Inc()
			return nil
		}
		lastErr = err

		reason, retryable := retryReason(err)
		if !retryable {
			db.metrics.PostgresTxTotal.WithLabelValues("rollback").Inc()
			return err
		}
		db.metrics.PostgresRetries.WithLabelValues(reason).Inc()
		observability.Logger(ctx).Warn("retrying postgres transaction",
			"attempt", attempt+1, "reason", reason, "error", err.Error())
	}

	db.metrics.PostgresTxTotal.WithLabelValues("rollback").Inc()
	return fmt.Errorf("storage: transaction failed after %d attempts: %w", db.maxRetries, lastErr)
}

func (db *DB) runOnce(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) (err error) {
	tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("storage: begin: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			// Roll back before re-panicking so a panicking failpoint cannot
			// leave a transaction open on the connection.
			_ = tx.Rollback(context.WithoutCancel(ctx))
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	if err = fn(ctx, tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("storage: commit: %w", err)
	}
	return nil
}

// retryReason classifies an error as retryable and names the reason.
func retryReason(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}
	switch pgErr.Code {
	case "40P01": // deadlock_detected
		return "deadlock", true
	case "40001": // serialization_failure
		return "serialization", true
	case "55P03": // lock_not_available
		return "lock_timeout", true
	case "57014": // query_canceled — includes lock_timeout expiry
		return "lock_timeout", true
	default:
		return "", false
	}
}

// IsUniqueViolation reports whether err is a unique constraint violation,
// optionally restricted to a specific constraint name.
func IsUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	return constraint == "" || pgErr.ConstraintName == constraint
}

// IsCheckViolation reports whether err violates a CHECK constraint, optionally
// restricted to a specific constraint name.
func IsCheckViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		return false
	}
	return constraint == "" || pgErr.ConstraintName == constraint
}

// IsForeignKeyViolation reports whether err is an FK violation.
func IsForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}

// backoff returns an exponentially growing delay with full jitter.
func backoff(attempt int) time.Duration {
	base := 5 * time.Millisecond
	maxDelay := 500 * time.Millisecond
	delay := base << attempt
	if delay > maxDelay {
		delay = maxDelay
	}
	// Full jitter avoids retry convoys when many transfers collide at once.
	return time.Duration(rand.Int63n(int64(delay)) + int64(base))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func msString(d time.Duration) string {
	return fmt.Sprintf("%d", d.Milliseconds())
}
