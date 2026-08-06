// Package kafka wraps the Kafka producer and consumer used for payment events.
//
// Delivery is AT-LEAST-ONCE. This project deliberately does not claim
// exactly-once messaging; exactly-once *ledger effects* come from PostgreSQL
// transactions and idempotency, and every consumer deduplicates on event ID.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/observability"
)

// Producer publishes events to Kafka.
type Producer struct {
	writer  *kafka.Writer
	metrics *observability.Metrics
	timeout time.Duration
}

// Message is a single Kafka record.
type Message struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

// NewProducer builds a synchronous producer with acks=all.
func NewProducer(cfg *config.Config, metrics *observability.Metrics) *Producer {
	writer := &kafka.Writer{
		Addr: kafka.TCP(cfg.KafkaBrokers...),
		// Balancer is per-key so all events for one transaction land on the
		// same partition and stay in order relative to each other.
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		// The outbox owns retries, with durable attempt counts and backoff;
		// letting the client retry too would double-count attempts.
		MaxAttempts:            1,
		Async:                  false,
		AllowAutoTopicCreation: true,
		WriteTimeout:           cfg.KafkaTimeout,
		ReadTimeout:            cfg.KafkaTimeout,
		BatchTimeout:           10 * time.Millisecond,
	}
	return &Producer{writer: writer, metrics: metrics, timeout: cfg.KafkaTimeout}
}

// Publish writes a single message, returning an error the caller can retry.
func (p *Producer) Publish(ctx context.Context, msg Message) error {
	writeCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	headers := make([]kafka.Header, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, kafka.Header{Key: k, Value: []byte(v)})
	}

	err := p.writer.WriteMessages(writeCtx, kafka.Message{
		Topic:   msg.Topic,
		Key:     []byte(msg.Key),
		Value:   msg.Value,
		Headers: headers,
	})
	if err != nil {
		p.metrics.KafkaPublishTotal.WithLabelValues(msg.Topic, "failure").Inc()
		return fmt.Errorf("kafka: publish to %s: %w", msg.Topic, err)
	}
	p.metrics.KafkaPublishTotal.WithLabelValues(msg.Topic, "success").Inc()
	return nil
}

// Close flushes and shuts down the producer.
func (p *Producer) Close() error { return p.writer.Close() }

// Ping checks broker reachability for readiness probes.
func (p *Producer) Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return errors.New("kafka: no brokers configured")
	}
	dialer := &kafka.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("kafka: dial %s: %w", brokers[0], err)
	}
	defer func() {
		_ = conn.Close()
	}()
	if _, err := conn.Brokers(); err != nil {
		return fmt.Errorf("kafka: metadata: %w", err)
	}
	return nil
}

// EnsureTopics creates the given topics if they do not exist. Safe to call
// repeatedly; "already exists" is not an error.
func EnsureTopics(ctx context.Context, brokers []string, partitions int, topics ...string) error {
	if len(brokers) == 0 {
		return errors.New("kafka: no brokers configured")
	}
	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("kafka: dial: %w", err)
	}
	defer func() {
		_ = conn.Close()
	}()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka: controller: %w", err)
	}
	ctrlConn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("kafka: dial controller: %w", err)
	}
	defer func() {
		_ = ctrlConn.Close()
	}()

	specs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		specs = append(specs, kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		})
	}
	if err := ctrlConn.CreateTopics(specs...); err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	return nil
}
