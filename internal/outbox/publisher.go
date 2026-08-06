package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/observability"
)

// Publisher drains the outbox to Kafka.
//
// It never reads or writes balances, entries or transactions: a publish retry
// storm cannot change a single cent of the ledger (INVARIANT 10).
type Publisher struct {
	pool     *pgxpool.Pool
	producer *kafka.Producer
	cfg      *config.Config
	metrics  *observability.Metrics
	fp       *failpoint.Registry
	workerID string
}

// NewPublisher builds a publisher.
func NewPublisher(pool *pgxpool.Pool, producer *kafka.Producer, cfg *config.Config, metrics *observability.Metrics, fp *failpoint.Registry) *Publisher {
	return &Publisher{
		pool:     pool,
		producer: producer,
		cfg:      cfg,
		metrics:  metrics,
		fp:       fp,
		workerID: cfg.OutboxWorkerID,
	}
}

// Run polls until the context is cancelled. On restart it simply picks up
// whatever is still pending or whose claim has expired, so in-flight work
// survives a crash with no external coordination.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.OutboxPollInterval)
	defer ticker.Stop()

	logger := observability.Logger(ctx)
	logger.Info("outbox publisher started", "worker_id", p.workerID, "batch_size", p.cfg.OutboxBatchSize)

	for {
		published, err := p.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("outbox publish cycle failed", "error", err.Error())
		}

		// Drain greedily while there is a full batch of work.
		if published >= p.cfg.OutboxBatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			logger.Info("outbox publisher stopping")
			return nil
		case <-ticker.C:
		}
	}
}

// RunOnce claims and publishes at most one batch, returning how many rows were
// published successfully.
func (p *Publisher) RunOnce(ctx context.Context) (int, error) {
	if n, err := Backlog(ctx, p.pool); err == nil {
		p.metrics.OutboxBacklog.Set(float64(n))
	}

	batch, err := Claim(ctx, p.pool, p.workerID, p.cfg.OutboxBatchSize, p.cfg.OutboxClaimTTL)
	if err != nil {
		return 0, err
	}

	var published int
	for _, rec := range batch {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		if p.publishOne(ctx, rec) {
			published++
		}
	}
	return published, nil
}

// publishOne publishes a single record and records the outcome. It returns
// true when the row reached the published state.
func (p *Publisher) publishOne(ctx context.Context, rec Record) bool {
	ctx = observability.WithTransactionID(ctx, rec.AggregateID)
	if cid := rec.Headers["correlation_id"]; cid != "" {
		ctx = observability.WithCorrelationID(ctx, cid)
	}
	logger := observability.Logger(ctx).With("event_id", rec.ID, "topic", rec.Topic)

	if err := p.fp.Eval(ctx, failpoint.BeforeKafkaPublish); err != nil {
		p.fail(ctx, rec, err, logger)
		return false
	}

	err := p.producer.Publish(ctx, kafka.Message{
		Topic:   rec.Topic,
		Key:     rec.PartitionKey,
		Value:   rec.Payload,
		Headers: withEventHeaders(rec),
	})
	if err != nil {
		p.fail(ctx, rec, err, logger)
		return false
	}

	// Crashing here republishes the event later. That is at-least-once by
	// design: consumers deduplicate on event_id.
	if err := p.fp.Eval(ctx, failpoint.AfterKafkaPublish); err != nil {
		logger.Warn("crashed after kafka publish before marking outbox row; event will be republished",
			"error", err.Error())
		return false
	}

	if err := MarkPublished(ctx, p.pool, rec.ID); err != nil {
		logger.Error("published to kafka but failed to mark outbox row", "error", err.Error())
		return false
	}
	logger.Info("published outbox event", "attempts", rec.Attempts+1)
	return true
}

func (p *Publisher) fail(ctx context.Context, rec Record, cause error, logger interface {
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}) {
	p.metrics.KafkaRetryTotal.WithLabelValues(rec.Topic).Inc()

	delay := Backoff(rec.Attempts+1, p.cfg.OutboxBaseBackoff, p.cfg.OutboxMaxBackoff)
	attempts, dead, err := MarkFailed(ctx, p.pool, rec.ID, cause.Error(), delay, p.cfg.OutboxMaxAttempts)
	if err != nil {
		logger.Error("failed to record outbox failure", "error", err.Error())
		return
	}
	if dead {
		p.metrics.OutboxDeadLetters.Inc()
		logger.Error("outbox event exhausted retries and was dead-lettered",
			"attempts", attempts, "error", cause.Error())
		p.publishToDLQ(ctx, rec, cause)
		return
	}
	logger.Warn("outbox publish failed; scheduled retry",
		"attempts", attempts, "retry_in", delay.String(), "error", cause.Error())
}

// publishToDLQ makes a single best-effort attempt to record the dead letter on
// the DLQ topic. Failure is expected when Kafka is the thing that is down, and
// the dead_letter row in PostgreSQL remains the durable record either way.
func (p *Publisher) publishToDLQ(ctx context.Context, rec Record, cause error) {
	payload, err := json.Marshal(map[string]any{
		"event_id":         rec.ID,
		"topic":            rec.Topic,
		"event_type":       rec.EventType,
		"transaction_id":   rec.AggregateID,
		"attempts":         rec.Attempts + 1,
		"error":            cause.Error(),
		"original":         rec.Payload,
		"dead_lettered_at": time.Now().UTC(),
	})
	if err != nil {
		return
	}
	dlqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.KafkaTimeout)
	defer cancel()
	if err := p.producer.Publish(dlqCtx, kafka.Message{
		Topic:   p.cfg.TopicPaymentsDLQ,
		Key:     rec.PartitionKey,
		Value:   payload,
		Headers: withEventHeaders(rec),
	}); err != nil {
		observability.Logger(ctx).Error("failed to publish dead letter to DLQ topic",
			"event_id", rec.ID, "error", err.Error())
	}
}

func withEventHeaders(rec Record) map[string]string {
	headers := make(map[string]string, len(rec.Headers)+3)
	for k, v := range rec.Headers {
		headers[k] = v
	}
	headers["event_id"] = rec.ID
	headers["transaction_id"] = rec.AggregateID
	headers["event_type"] = rec.EventType
	return headers
}

// Backoff returns an exponentially increasing delay with full jitter, capped
// at maxDelay. Jitter matters: without it, every event that failed during the
// same Kafka outage would retry in lockstep and stampede the broker on
// recovery.
func Backoff(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	if exp > float64(maxDelay) || math.IsInf(exp, 1) {
		exp = float64(maxDelay)
	}
	// Full jitter over [base/2, exp].
	minDelay := float64(base) / 2
	if exp <= minDelay {
		return time.Duration(exp)
	}
	return time.Duration(minDelay + rand.Float64()*(exp-minDelay))
}
