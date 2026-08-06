package kafka

import (
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/observability"
)

// Handler processes one consumed message. Returning an error prevents the
// offset from being committed, so the message is redelivered — which is safe
// precisely because handlers are idempotent.
type Handler func(ctx context.Context, msg Message) error

// Consumer reads payment events from one or more topics as part of a group.
type Consumer struct {
	reader  *kafka.Reader
	metrics *observability.Metrics
	topics  []string
}

// NewConsumer builds a consumer-group reader with manual offset commits.
func NewConsumer(cfg *config.Config, group string, topics []string, metrics *observability.Metrics) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		GroupID:     group,
		GroupTopics: topics,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
		// Offsets are committed only after the handler has durably persisted
		// its work, so a crash replays rather than skips.
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	return &Consumer{reader: reader, metrics: metrics, topics: topics}
}

// Run consumes until the context is cancelled.
//
// A broker outage must not kill the consumer: a failed fetch is retried with
// bounded backoff rather than returned. Exiting here would strand the worker
// permanently after a transient Kafka restart, which is exactly the failure
// this system is supposed to survive. Run therefore returns only on shutdown
// or on an unrecoverable reader state.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	var failures int

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			// The reader has been closed; there is nothing to reconnect to.
			if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, kafka.ErrGroupClosed) {
				return nil
			}

			failures++
			c.metrics.KafkaConsumeTotal.WithLabelValues("", "fetch_error").Inc()
			delay := fetchBackoff(failures)
			observability.Logger(ctx).Warn("kafka fetch failed; retrying",
				"consecutive_failures", failures, "retry_in", delay.String(), "error", err.Error())

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(delay):
			}
			continue
		}
		failures = 0

		headers := make(map[string]string, len(msg.Headers))
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}

		err = handle(ctx, Message{
			Topic:   msg.Topic,
			Key:     string(msg.Key),
			Value:   msg.Value,
			Headers: headers,
		})
		if err != nil {
			c.metrics.KafkaConsumeTotal.WithLabelValues(msg.Topic, "failure").Inc()
			observability.Logger(ctx).Error("failed to handle kafka message; offset not committed",
				"topic", msg.Topic, "offset", msg.Offset, "error", err.Error())
			// Do not commit: the message will be redelivered.
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			// The work is already durable; a failed commit only means the
			// message is redelivered and deduplicated downstream.
			observability.Logger(ctx).Warn("failed to commit kafka offset",
				"topic", msg.Topic, "offset", msg.Offset, "error", err.Error())
		}
		c.metrics.KafkaConsumeTotal.WithLabelValues(msg.Topic, "success").Inc()
	}
}

// fetchBackoff grows the pause between failed fetches, capped at 30s, with
// jitter so replicas of the same consumer group do not reconnect in lockstep
// the instant a broker returns.
func fetchBackoff(failures int) time.Duration {
	const (
		base     = 250 * time.Millisecond
		maxDelay = 30 * time.Second
	)
	if failures < 1 {
		failures = 1
	}
	delay := float64(base) * math.Pow(2, float64(failures-1))
	if delay > float64(maxDelay) || math.IsInf(delay, 1) {
		delay = float64(maxDelay)
	}
	half := delay / 2
	return time.Duration(half + rand.Float64()*half)
}

// Close shuts down the reader.
func (c *Consumer) Close() error { return c.reader.Close() }
