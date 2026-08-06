package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/outbox"
)

// Worker consumes payment events and drives webhook delivery.
//
// Consumption and delivery are separated on purpose: the Kafka handler only
// persists intent (a pending row), and a poller performs the actual HTTP
// calls. That way a slow or failing endpoint never blocks the consumer group,
// and every retry survives a restart because the state lives in PostgreSQL.
type Worker struct {
	pool      *pgxpool.Pool
	cfg       *config.Config
	metrics   *observability.Metrics
	fp        *failpoint.Registry
	client    *http.Client
	endpoints []string
}

// NewWorker builds a webhook worker.
func NewWorker(pool *pgxpool.Pool, cfg *config.Config, metrics *observability.Metrics, fp *failpoint.Registry) *Worker {
	return &Worker{
		pool:      pool,
		cfg:       cfg,
		metrics:   metrics,
		fp:        fp,
		client:    &http.Client{Timeout: cfg.WebhookTimeout},
		endpoints: cfg.WebhookEndpoints,
	}
}

// HandleEvent is the Kafka handler. It records one pending delivery per
// configured endpoint and returns; the poller does the rest.
func (w *Worker) HandleEvent(ctx context.Context, msg kafka.Message) error {
	var env outbox.Envelope
	if err := json.Unmarshal(msg.Value, &env); err != nil {
		// A malformed message will never become valid; committing the offset
		// (by returning nil) avoids an infinite redelivery loop.
		observability.Logger(ctx).Error("discarding malformed payment event",
			"topic", msg.Topic, "error", err.Error())
		return nil
	}
	if env.EventID == "" {
		observability.Logger(ctx).Error("discarding payment event with no event_id", "topic", msg.Topic)
		return nil
	}

	ctx = observability.WithTransactionID(ctx, env.TransactionID)
	if env.CorrelationID != "" {
		ctx = observability.WithCorrelationID(ctx, env.CorrelationID)
	}
	logger := observability.Logger(ctx).With("event_id", env.EventID, "event_type", env.EventType)

	if err := w.fp.Eval(ctx, failpoint.BeforeWebhookPersist); err != nil {
		// Returning an error leaves the offset uncommitted, so the event is
		// redelivered and the delivery row is created on the next attempt.
		return fmt.Errorf("webhook: failpoint before persist: %w", err)
	}

	for _, endpoint := range w.endpoints {
		created, err := Enqueue(ctx, w.pool, Delivery{
			ID:            uuid.NewString(),
			EventID:       env.EventID,
			Endpoint:      endpoint,
			EventType:     env.EventType,
			TransactionID: env.TransactionID,
			Payload:       msg.Value,
		})
		if err != nil {
			return err
		}
		if !created {
			// Either Kafka redelivered the event or it was replayed; either
			// way the existing row is the single source of truth.
			w.metrics.WebhookDuplicates.Inc()
			logger.Info("duplicate payment event ignored", "endpoint", endpoint,
				"replay", env.Replay != nil && env.Replay.IsReplay)
			continue
		}
		logger.Info("queued webhook delivery", "endpoint", endpoint)
	}
	return nil
}

// RunDeliveries polls for due deliveries until the context is cancelled. On
// startup it naturally picks up anything left pending by a previous process.
func (w *Worker) RunDeliveries(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.WebhookPollInterval)
	defer ticker.Stop()

	logger := observability.Logger(ctx)
	logger.Info("webhook delivery loop started", "endpoints", w.endpoints)

	for {
		if _, err := w.DeliverOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("webhook delivery cycle failed", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			logger.Info("webhook delivery loop stopping")
			return nil
		case <-ticker.C:
		}
	}
}

// DeliverOnce processes one batch of due deliveries.
func (w *Worker) DeliverOnce(ctx context.Context) (int, error) {
	batch, err := ClaimDue(ctx, w.pool, w.cfg.WebhookBatchSize)
	if err != nil {
		return 0, err
	}
	var delivered int
	for _, d := range batch {
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
		if w.deliver(ctx, d) {
			delivered++
		}
	}
	return delivered, nil
}

func (w *Worker) deliver(ctx context.Context, d Delivery) bool {
	ctx = observability.WithTransactionID(ctx, d.TransactionID)
	logger := observability.Logger(ctx).With(
		"event_id", d.EventID, "endpoint", d.Endpoint, "attempt", d.Attempts+1)

	if err := w.fp.Eval(ctx, failpoint.BeforeWebhookSend); err != nil {
		w.recordFailure(ctx, d, err.Error(), 0, logger)
		return false
	}

	statusCode, err := w.post(ctx, d)
	if err != nil {
		w.recordFailure(ctx, d, err.Error(), statusCode, logger)
		return false
	}

	if err := MarkDelivered(ctx, w.pool, d.ID, statusCode); err != nil {
		logger.Error("delivered webhook but failed to record it", "error", err.Error())
		return false
	}
	w.metrics.WebhookDeliveries.WithLabelValues("success").Inc()
	logger.Info("webhook delivered", "status_code", statusCode)
	return true
}

func (w *Worker) recordFailure(ctx context.Context, d Delivery, cause string, statusCode int, logger interface {
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}) {
	delay := outbox.Backoff(d.Attempts+1, w.cfg.WebhookBaseBackoff, w.cfg.WebhookMaxBackoff)
	attempts, dead, err := MarkFailed(ctx, w.pool, d.ID, cause, statusCode, delay, w.cfg.WebhookMaxAttempts)
	if err != nil {
		logger.Error("failed to record webhook failure", "error", err.Error())
		return
	}
	if dead {
		w.metrics.WebhookDeliveries.WithLabelValues("dead_letter").Inc()
		logger.Error("webhook delivery exhausted retries; moved to dead letter",
			"attempts", attempts, "error", cause)
		return
	}
	w.metrics.WebhookDeliveries.WithLabelValues("retry").Inc()
	logger.Warn("webhook delivery failed; scheduled retry",
		"attempts", attempts, "retry_in", delay.String(), "error", cause)
}

// post performs the HTTP call. Any non-2xx is treated as a retryable failure.
func (w *Worker) post(ctx context.Context, d Delivery) (int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, w.cfg.WebhookTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, d.Endpoint, bytes.NewReader(d.Payload))
	if err != nil {
		return 0, fmt.Errorf("webhook: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Ledger-Event-Id", d.EventID)
	req.Header.Set("X-Ledger-Event-Type", d.EventType)
	req.Header.Set("X-Ledger-Transaction-Id", d.TransactionID)
	req.Header.Set("X-Ledger-Delivery-Attempt", fmt.Sprintf("%d", d.Attempts+1))
	if secret := w.cfg.WebhookSigningSecret; secret != "" {
		req.Header.Set("X-Ledger-Signature", sign(secret, d.Payload))
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("webhook: post: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("webhook: endpoint returned %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

// sign returns the HMAC-SHA256 signature of the payload, letting receivers
// verify the event really came from this ledger.
func sign(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
