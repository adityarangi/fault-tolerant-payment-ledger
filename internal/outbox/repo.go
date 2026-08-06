package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Insert writes an outbox row inside the caller's transaction. This is the
// only way events enter the system: an event exists if and only if the ledger
// change that produced it committed.
func Insert(ctx context.Context, tx pgx.Tx, rec Record) error {
	headers := rec.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	headerJSON, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("outbox: marshal headers: %w", err)
	}
	_, err = tx.Exec(ctx, `
        INSERT INTO outbox_events (id, topic, event_type, aggregate_id, partition_key, payload, headers, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		rec.ID, rec.Topic, rec.EventType, rec.AggregateID, rec.PartitionKey,
		[]byte(rec.Payload), headerJSON, StatusPending)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

// Claim atomically reserves up to limit due rows for this worker.
//
// FOR UPDATE SKIP LOCKED is what makes multiple publisher replicas safe: each
// worker takes a disjoint set of rows and never blocks on another worker's
// batch. The claim carries a TTL so a crashed worker's rows become claimable
// again without any external coordination (INVARIANT 10 keeps this harmless:
// claiming and publishing never touch balances).
func Claim(ctx context.Context, pool *pgxpool.Pool, workerID string, limit int, claimTTL time.Duration) ([]Record, error) {
	rows, err := pool.Query(ctx, `
        WITH claimed AS (
            SELECT id
              FROM outbox_events
             WHERE status IN ('pending', 'publishing')
               AND next_attempt_at <= now()
               AND (status = 'pending' OR claimed_at IS NULL OR claimed_at < now() - $3::interval)
             ORDER BY created_at, id
             FOR UPDATE SKIP LOCKED
             LIMIT $2
        )
        UPDATE outbox_events o
           SET status = 'publishing', claimed_by = $1, claimed_at = now()
          FROM claimed
         WHERE o.id = claimed.id
     RETURNING o.id::text, o.topic, o.event_type, o.aggregate_id, o.partition_key,
               o.payload, o.headers, o.status, o.attempts,
               COALESCE(o.last_error, ''), o.next_attempt_at, o.created_at`,
		workerID, limit, claimTTL.String())
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// MarkPublished records a successful Kafka publication.
//
// Crashing between the Kafka write and this update simply means the event is
// republished later: Kafka delivery is at-least-once by design and consumers
// deduplicate on event ID.
func MarkPublished(ctx context.Context, pool *pgxpool.Pool, id string) error {
	_, err := pool.Exec(ctx, `
        UPDATE outbox_events
           SET status = 'published', published_at = now(), attempts = attempts + 1,
               last_error = NULL, claimed_by = NULL, claimed_at = NULL
         WHERE id = $1 AND status <> 'published'`, id)
	if err != nil {
		return fmt.Errorf("outbox: mark published: %w", err)
	}
	return nil
}

// MarkFailed schedules a retry, or moves the row to dead_letter once the
// attempt budget is exhausted. It returns the new attempt count and whether
// the row was dead-lettered.
func MarkFailed(ctx context.Context, pool *pgxpool.Pool, id string, cause string, backoff time.Duration, maxAttempts int) (attempts int, deadLettered bool, err error) {
	var status string
	err = pool.QueryRow(ctx, `
        UPDATE outbox_events
           SET attempts = attempts + 1,
               last_error = $2,
               claimed_by = NULL,
               claimed_at = NULL,
               status = CASE WHEN attempts + 1 >= $4 THEN 'dead_letter' ELSE 'pending' END,
               next_attempt_at = now() + $3::interval
         WHERE id = $1
     RETURNING attempts, status`, id, truncate(cause, 2000), backoff.String(), maxAttempts).
		Scan(&attempts, &status)
	if err != nil {
		return 0, false, fmt.Errorf("outbox: mark failed: %w", err)
	}
	return attempts, status == StatusDeadLetter, nil
}

// Backlog counts rows still awaiting publication, for the backlog gauge.
func Backlog(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE status IN ('pending', 'publishing')`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("outbox: backlog: %w", err)
	}
	return n, nil
}

// Get loads a single outbox row.
func Get(ctx context.Context, pool *pgxpool.Pool, id string) (Record, error) {
	rows, err := pool.Query(ctx, `
        SELECT id::text, topic, event_type, aggregate_id, partition_key, payload, headers,
               status, attempts, COALESCE(last_error, ''), next_attempt_at, created_at
          FROM outbox_events WHERE id = $1`, id)
	if err != nil {
		return Record{}, fmt.Errorf("outbox: get: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return Record{}, pgx.ErrNoRows
	}
	return scanRecord(rows)
}

// ListForReplay returns published history in deterministic order, filtered by
// transaction ID and/or a time range. Replay reads only; it never writes to
// the ledger.
func ListForReplay(ctx context.Context, pool *pgxpool.Pool, transactionID string, from, to *time.Time, limit int) ([]Record, error) {
	rows, err := pool.Query(ctx, `
        SELECT id::text, topic, event_type, aggregate_id, partition_key, payload, headers,
               status, attempts, COALESCE(last_error, ''), next_attempt_at, created_at
          FROM outbox_events
         WHERE ($1 = '' OR aggregate_id = $1)
           AND ($2::timestamptz IS NULL OR created_at >= $2)
           AND ($3::timestamptz IS NULL OR created_at <= $3)
         ORDER BY created_at, id
         LIMIT $4`, transactionID, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: list for replay: %w", err)
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func scanRecord(rows pgx.Rows) (Record, error) {
	var rec Record
	var payload, headers []byte
	if err := rows.Scan(&rec.ID, &rec.Topic, &rec.EventType, &rec.AggregateID, &rec.PartitionKey,
		&payload, &headers, &rec.Status, &rec.Attempts, &rec.LastError,
		&rec.NextAttemptAt, &rec.CreatedAt); err != nil {
		return Record{}, fmt.Errorf("outbox: scan: %w", err)
	}
	rec.Payload = payload
	rec.Headers = map[string]string{}
	if len(headers) > 0 {
		_ = json.Unmarshal(headers, &rec.Headers)
	}
	return rec, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
