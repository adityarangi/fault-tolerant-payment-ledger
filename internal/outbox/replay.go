package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/internal/apperr"
	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/observability"
)

// ReplayRequest selects which historical events to republish.
type ReplayRequest struct {
	TransactionID string     `json:"transaction_id,omitempty"`
	From          *time.Time `json:"from,omitempty"`
	To            *time.Time `json:"to,omitempty"`
	Limit         int        `json:"limit,omitempty"`
	RequestedBy   string     `json:"requested_by,omitempty"`
	// DryRun lists what would be replayed without publishing anything.
	DryRun bool `json:"dry_run,omitempty"`
}

// ReplayResult reports what a replay did.
type ReplayResult struct {
	ReplayID  string   `json:"replay_id"`
	Matched   int      `json:"matched"`
	Published int      `json:"published"`
	Failed    int      `json:"failed"`
	DryRun    bool     `json:"dry_run"`
	EventIDs  []string `json:"event_ids"`
}

// Replayer republishes historical ledger events to Kafka.
//
// Replay is strictly a read of outbox history plus a Kafka write. It never
// opens a ledger transaction, never touches balances and never creates
// entries (INVARIANT 10). Because the original event ID is preserved, an
// idempotent consumer treats a replayed event as a duplicate of the original
// and does no new work.
type Replayer struct {
	pool     *pgxpool.Pool
	producer *kafka.Producer
	cfg      *config.Config
	metrics  *observability.Metrics
}

// NewReplayer builds a replayer.
func NewReplayer(pool *pgxpool.Pool, producer *kafka.Producer, cfg *config.Config, metrics *observability.Metrics) *Replayer {
	return &Replayer{pool: pool, producer: producer, cfg: cfg, metrics: metrics}
}

// Validate checks a replay request.
func (r *ReplayRequest) Validate() error {
	if r.TransactionID == "" && r.From == nil && r.To == nil {
		return apperr.InvalidRequest("replay requires transaction_id and/or a from/to time range")
	}
	if r.From != nil && r.To != nil && r.To.Before(*r.From) {
		return apperr.InvalidRequest("to must not be before from")
	}
	if r.Limit < 0 || r.Limit > 10000 {
		return apperr.InvalidRequest("limit must be between 0 and 10000")
	}
	return nil
}

// Replay republishes matching events in deterministic (created_at, id) order.
func (r *Replayer) Replay(ctx context.Context, req ReplayRequest) (*ReplayResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = 1000
	}

	records, err := ListForReplay(ctx, r.pool, req.TransactionID, req.From, req.To, limit)
	if err != nil {
		return nil, err
	}

	replayID := uuid.NewString()
	replayedAt := time.Now().UTC()
	result := &ReplayResult{
		ReplayID: replayID,
		Matched:  len(records),
		DryRun:   req.DryRun,
		EventIDs: make([]string, 0, len(records)),
	}
	logger := observability.Logger(ctx).With("replay_id", replayID)

	for i, rec := range records {
		result.EventIDs = append(result.EventIDs, rec.ID)
		if req.DryRun {
			continue
		}

		payload, err := markReplayed(rec.Payload, ReplayMeta{
			IsReplay:    true,
			ReplayID:    replayID,
			ReplayedAt:  replayedAt,
			ReplayedBy:  req.RequestedBy,
			OriginalSeq: int64(i),
		})
		if err != nil {
			result.Failed++
			r.metrics.ReplayEvents.WithLabelValues("failure").Inc()
			logger.Error("failed to annotate replayed event", "event_id", rec.ID, "error", err.Error())
			continue
		}

		headers := withEventHeaders(rec)
		headers["replay"] = "true"
		headers["replay_id"] = replayID

		if err := r.producer.Publish(ctx, kafka.Message{
			Topic:   rec.Topic,
			Key:     rec.PartitionKey,
			Value:   payload,
			Headers: headers,
		}); err != nil {
			result.Failed++
			r.metrics.ReplayEvents.WithLabelValues("failure").Inc()
			logger.Error("failed to replay event", "event_id", rec.ID, "error", err.Error())
			continue
		}
		result.Published++
		r.metrics.ReplayEvents.WithLabelValues("success").Inc()
	}

	logger.Info("replay complete",
		"matched", result.Matched, "published", result.Published,
		"failed", result.Failed, "dry_run", req.DryRun)
	return result, nil
}

// markReplayed adds replay metadata to an event envelope while preserving the
// original event ID, so consumers still deduplicate it against the original.
func markReplayed(payload json.RawMessage, meta ReplayMeta) ([]byte, error) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("outbox: decode envelope for replay: %w", err)
	}
	env.Replay = &meta
	out, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("outbox: encode replayed envelope: %w", err)
	}
	return out, nil
}
