// Package webhook consumes payment events from Kafka and delivers them to
// configured HTTP endpoints with durable, retryable delivery state.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/internal/storage"
)

// Delivery states.
const (
	StatusPending    = "pending"
	StatusDelivered  = "delivered"
	StatusDeadLetter = "dead_letter"
)

// Delivery is a row of webhook_deliveries.
type Delivery struct {
	ID             string          `json:"id"`
	EventID        string          `json:"event_id"`
	Endpoint       string          `json:"endpoint"`
	EventType      string          `json:"event_type"`
	TransactionID  string          `json:"transaction_id"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Attempts       int             `json:"attempts"`
	LastError      string          `json:"last_error,omitempty"`
	LastStatusCode int             `json:"last_status_code,omitempty"`
	NextAttemptAt  time.Time       `json:"next_attempt_at"`
	DeliveredAt    *time.Time      `json:"delivered_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// Enqueue records a pending delivery for one event/endpoint pair.
//
// The UNIQUE (event_id, endpoint) constraint makes this safe under Kafka's
// at-least-once delivery: a redelivered event finds the row already present
// and creates nothing new (INVARIANT 11). It reports whether the row was new.
func Enqueue(ctx context.Context, q storage.Querier, d Delivery) (created bool, err error) {
	tag, err := q.Exec(ctx, `
        INSERT INTO webhook_deliveries
            (id, event_id, endpoint, event_type, transaction_id, payload, status)
        VALUES ($1, $2, $3, $4, $5, $6, 'pending')
        ON CONFLICT (event_id, endpoint) DO NOTHING`,
		d.ID, d.EventID, d.Endpoint, d.EventType, d.TransactionID, []byte(d.Payload))
	if err != nil {
		return false, fmt.Errorf("webhook: enqueue: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ClaimDue locks up to limit deliveries that are due, skipping rows already
// being worked by another replica.
func ClaimDue(ctx context.Context, pool *pgxpool.Pool, limit int) ([]Delivery, error) {
	rows, err := pool.Query(ctx, `
        SELECT id::text, event_id::text, endpoint, event_type, transaction_id, payload,
               status, attempts, COALESCE(last_error, ''), COALESCE(last_status_code, 0),
               next_attempt_at, delivered_at, created_at
          FROM webhook_deliveries
         WHERE status = 'pending' AND next_attempt_at <= now()
         ORDER BY next_attempt_at, created_at
         LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("webhook: claim due: %w", err)
	}
	defer rows.Close()

	var out []Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MarkDelivered records a successful delivery. Repeating it is harmless, which
// is what makes duplicate successful deliveries idempotent.
func MarkDelivered(ctx context.Context, pool *pgxpool.Pool, id string, statusCode int) error {
	_, err := pool.Exec(ctx, `
        UPDATE webhook_deliveries
           SET status = 'delivered', delivered_at = COALESCE(delivered_at, now()),
               attempts = attempts + 1, last_status_code = $2, last_error = NULL,
               updated_at = now()
         WHERE id = $1 AND status <> 'delivered'`, id, statusCode)
	if err != nil {
		return fmt.Errorf("webhook: mark delivered: %w", err)
	}
	return nil
}

// MarkFailed schedules a retry or dead-letters an exhausted delivery.
func MarkFailed(ctx context.Context, pool *pgxpool.Pool, id, cause string, statusCode int, backoff time.Duration, maxAttempts int) (attempts int, dead bool, err error) {
	var status string
	err = pool.QueryRow(ctx, `
        UPDATE webhook_deliveries
           SET attempts = attempts + 1,
               last_error = $2,
               last_status_code = $3,
               status = CASE WHEN attempts + 1 >= $5 THEN 'dead_letter' ELSE 'pending' END,
               next_attempt_at = now() + $4::interval,
               updated_at = now()
         WHERE id = $1
     RETURNING attempts, status`, id, truncate(cause, 2000), statusCode, backoff.String(), maxAttempts).
		Scan(&attempts, &status)
	if err != nil {
		return 0, false, fmt.Errorf("webhook: mark failed: %w", err)
	}
	return attempts, status == StatusDeadLetter, nil
}

// Get loads a delivery by event ID and endpoint.
func Get(ctx context.Context, pool *pgxpool.Pool, eventID, endpoint string) (Delivery, error) {
	rows, err := pool.Query(ctx, `
        SELECT id::text, event_id::text, endpoint, event_type, transaction_id, payload,
               status, attempts, COALESCE(last_error, ''), COALESCE(last_status_code, 0),
               next_attempt_at, delivered_at, created_at
          FROM webhook_deliveries WHERE event_id = $1 AND endpoint = $2`, eventID, endpoint)
	if err != nil {
		return Delivery{}, fmt.Errorf("webhook: get: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return Delivery{}, pgx.ErrNoRows
	}
	return scanDelivery(rows)
}

// CountByStatus returns delivery counts grouped by status, used by tests and
// operational dashboards.
func CountByStatus(ctx context.Context, pool *pgxpool.Pool) (map[string]int, error) {
	rows, err := pool.Query(ctx, `SELECT status, count(*) FROM webhook_deliveries GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("webhook: count by status: %w", err)
	}
	defer rows.Close()

	out := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		out[status] = n
	}
	return out, rows.Err()
}

func scanDelivery(rows pgx.Rows) (Delivery, error) {
	var d Delivery
	var payload []byte
	if err := rows.Scan(&d.ID, &d.EventID, &d.Endpoint, &d.EventType, &d.TransactionID, &payload,
		&d.Status, &d.Attempts, &d.LastError, &d.LastStatusCode,
		&d.NextAttemptAt, &d.DeliveredAt, &d.CreatedAt); err != nil {
		return Delivery{}, fmt.Errorf("webhook: scan: %w", err)
	}
	d.Payload = payload
	return d, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
