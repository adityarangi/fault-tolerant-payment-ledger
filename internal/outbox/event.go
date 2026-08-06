// Package outbox implements the transactional outbox: payment events are
// written to PostgreSQL in the same transaction as the ledger changes, then
// drained to Kafka by an independent worker.
package outbox

import (
	"encoding/json"
	"time"
)

// Outbox row states.
const (
	StatusPending    = "pending"
	StatusPublishing = "publishing"
	StatusPublished  = "published"
	StatusDeadLetter = "dead_letter"
)

// SchemaVersion is the envelope version carried by every event.
const SchemaVersion = 1

// Envelope is the payload published to Kafka.
//
// EventID is stable for the lifetime of the event, including across replays,
// so consumers can deduplicate on it alone.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	TransactionID string          `json:"transaction_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Replay        *ReplayMeta     `json:"replay,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// ReplayMeta marks an event as a republication of history rather than a new
// business fact. Consumers use it for observability; correctness still comes
// from deduplicating on EventID.
type ReplayMeta struct {
	IsReplay    bool      `json:"is_replay"`
	ReplayID    string    `json:"replay_id"`
	ReplayedAt  time.Time `json:"replayed_at"`
	ReplayedBy  string    `json:"replayed_by,omitempty"`
	OriginalSeq int64     `json:"original_seq,omitempty"`
}

// PaymentData is the business payload of a payment event.
type PaymentData struct {
	TransactionID         string      `json:"transaction_id"`
	Kind                  string      `json:"kind"`
	Currency              string      `json:"currency"`
	Amount                int64       `json:"amount"`
	SourceAccountID       string      `json:"source_account_id,omitempty"`
	DestinationAccountID  string      `json:"destination_account_id,omitempty"`
	Description           string      `json:"description,omitempty"`
	ExternalReference     string      `json:"external_reference,omitempty"`
	ReversesTransactionID string      `json:"reverses_transaction_id,omitempty"`
	Reason                string      `json:"reason,omitempty"`
	CreatedAt             time.Time   `json:"created_at"`
	Entries               []EntryData `json:"entries"`
}

// EntryData is a single posting as exposed to consumers.
type EntryData struct {
	EntryID   string `json:"entry_id"`
	AccountID string `json:"account_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Seq       int    `json:"seq"`
}

// Record is a row of the outbox_events table.
type Record struct {
	ID            string            `json:"id"`
	Topic         string            `json:"topic"`
	EventType     string            `json:"event_type"`
	AggregateID   string            `json:"aggregate_id"`
	PartitionKey  string            `json:"partition_key"`
	Payload       json.RawMessage   `json:"payload"`
	Headers       map[string]string `json:"headers"`
	Status        string            `json:"status"`
	Attempts      int               `json:"attempts"`
	LastError     string            `json:"last_error,omitempty"`
	NextAttemptAt time.Time         `json:"next_attempt_at"`
	PublishedAt   *time.Time        `json:"published_at,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
}
