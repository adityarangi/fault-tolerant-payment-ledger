// Package idempotency implements durable, PostgreSQL-backed idempotency for
// every mutating API call, with Redis as a strictly optional read cache.
//
// The durable record is written in the *same* transaction as the ledger
// changes it describes. That is what makes "at most one ledger transaction per
// key" a database guarantee rather than an application convention.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adityarangi/payment-ledger/internal/apperr"
)

// States of a durable idempotency record.
const (
	StateInProgress = "in_progress"
	StateCompleted  = "completed"
)

// Record is the durable idempotency row.
type Record struct {
	Scope          string          `json:"scope"`
	Key            string          `json:"key"`
	RequestHash    string          `json:"request_hash"`
	State          string          `json:"state"`
	ResponseStatus int             `json:"response_status"`
	ResponseBody   json.RawMessage `json:"response_body"`
	TransactionID  *string         `json:"transaction_id,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// ErrNotFound is returned when no record exists for a scope/key.
var ErrNotFound = errors.New("idempotency: record not found")

// HashRequest returns a stable hash of the canonical request payload. Two
// requests are "the same request" iff their hashes match.
func HashRequest(scope string, payload any) (string, error) {
	// json.Marshal sorts map keys and is deterministic for structs, so the
	// canonical form is stable across processes and Go versions.
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("idempotency: hash request: %w", err)
	}
	sum := sha256.Sum256(append([]byte(scope+"\x00"), body...))
	return hex.EncodeToString(sum[:]), nil
}

// LockKey serialises concurrent requests that share an idempotency key by
// taking a transaction-scoped PostgreSQL advisory lock.
//
// This is an optimisation for clean 409/replay behaviour, not the correctness
// mechanism: the primary key on (scope, key) is what actually prevents a
// second ledger transaction. A hash collision therefore costs a little extra
// serialisation and nothing else. The lock is released automatically when the
// transaction commits or rolls back, so a crashed process cannot strand it.
func LockKey(ctx context.Context, tx pgx.Tx, scope, key string) error {
	h := fnv.New64a()
	_, _ = h.Write([]byte(scope))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	// int64 conversion is intentional: pg_advisory_xact_lock takes a bigint.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(h.Sum64())); err != nil {
		return fmt.Errorf("idempotency: advisory lock: %w", err)
	}
	return nil
}

// Fetch reads the durable record for a scope/key, returning ErrNotFound when
// absent. Safe to call on a pool or inside a transaction.
func Fetch(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, scope, key string) (*Record, error) {
	var rec Record
	var body []byte
	var status *int
	err := q.QueryRow(ctx, `
        SELECT scope, key, request_hash, state, response_status, response_body,
               transaction_id::text, created_at
          FROM idempotency_records
         WHERE scope = $1 AND key = $2`, scope, key).
		Scan(&rec.Scope, &rec.Key, &rec.RequestHash, &rec.State, &status, &body,
			&rec.TransactionID, &rec.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("idempotency: fetch: %w", err)
	}
	if status != nil {
		rec.ResponseStatus = *status
	}
	rec.ResponseBody = body
	return &rec, nil
}

// Claim inserts an in-progress record for scope/key inside the caller's
// transaction.
//
// It returns the existing record when one is already present, so the caller
// can decide between replay (same hash) and conflict (different hash).
// Callers must hold LockKey for the same scope/key first.
func Claim(ctx context.Context, tx pgx.Tx, scope, key, requestHash string) (existing *Record, err error) {
	tag, err := tx.Exec(ctx, `
        INSERT INTO idempotency_records (scope, key, request_hash, state)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (scope, key) DO NOTHING`, scope, key, requestHash, StateInProgress)
	if err != nil {
		return nil, fmt.Errorf("idempotency: claim: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	return Fetch(ctx, tx, scope, key)
}

// Complete marks a claimed record completed and stores the response that must
// be replayed for future retries. Runs in the ledger's transaction.
func Complete(ctx context.Context, tx pgx.Tx, scope, key string, status int, body []byte, transactionID *string) error {
	_, err := tx.Exec(ctx, `
        UPDATE idempotency_records
           SET state = $3, response_status = $4, response_body = $5,
               transaction_id = $6, updated_at = now()
         WHERE scope = $1 AND key = $2`,
		scope, key, StateCompleted, status, body, transactionID)
	if err != nil {
		return fmt.Errorf("idempotency: complete: %w", err)
	}
	return nil
}

// Discard removes a still-in-progress claim so the key can be retried.
//
// It only ever deletes in_progress rows: a completed record is a durable
// result and must never be withdrawn.
func Discard(ctx context.Context, q interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}, scope, key string) error {
	_, err := q.Exec(ctx, `
        DELETE FROM idempotency_records
         WHERE scope = $1 AND key = $2 AND state = $3`, scope, key, StateInProgress)
	if err != nil {
		return fmt.Errorf("idempotency: discard: %w", err)
	}
	return nil
}

// Conflict builds the standard idempotency_conflict error.
func Conflict(key string) *apperr.Error {
	return apperr.New(apperr.CodeIdempotencyConflict,
		"idempotency key %q was already used with a different request payload", key).
		WithDetail("idempotency_key", key)
}

// InProgress builds the standard "retry shortly" error for a key whose
// original request is still executing.
func InProgress(key string) *apperr.Error {
	return apperr.New(apperr.CodeIdempotencyInProgress,
		"a request with idempotency key %q is already in progress", key).
		WithDetail("idempotency_key", key)
}
