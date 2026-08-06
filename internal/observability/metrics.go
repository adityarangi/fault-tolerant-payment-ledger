package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every Prometheus collector used by the system. It is passed
// explicitly rather than registered globally so tests can build isolated
// instances without duplicate-registration panics.
type Metrics struct {
	registry *prometheus.Registry

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec

	TransfersTotal   *prometheus.CounterVec // outcome=success|failure, reason=<code>
	ReversalsTotal   *prometheus.CounterVec
	IdempotencyTotal *prometheus.CounterVec // result=hit|miss|conflict|in_progress

	PostgresRetries *prometheus.CounterVec // reason=deadlock|serialization|lock_timeout
	PostgresTxTotal *prometheus.CounterVec // outcome=commit|rollback

	RedisOps *prometheus.CounterVec // op=..., result=hit|miss|error

	KafkaPublishTotal *prometheus.CounterVec // topic, outcome
	KafkaRetryTotal   *prometheus.CounterVec // topic
	KafkaConsumeTotal *prometheus.CounterVec // topic, outcome

	OutboxBacklog     prometheus.Gauge
	OutboxDeadLetters prometheus.Counter

	WebhookDeliveries *prometheus.CounterVec // outcome=success|retry|dead_letter
	WebhookDuplicates prometheus.Counter

	ReplayEvents *prometheus.CounterVec // outcome

	ReconciliationMismatches prometheus.Counter
	ReconciliationRuns       *prometheus.CounterVec

	RateLimited *prometheus.CounterVec
}

// NewMetrics builds and registers all collectors on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		reg.MustRegister(c)
		return c
	}
	plainCounter := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		reg.MustRegister(c)
		return c
	}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		reg.MustRegister(g)
		return g
	}

	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ledger_http_request_duration_seconds",
		Help:    "HTTP request latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
	reg.MustRegister(histogram)

	return &Metrics{
		registry: reg,

		HTTPRequests: counter("ledger_http_requests_total", "HTTP requests served.", "method", "route", "status", "code"),
		HTTPDuration: histogram,

		TransfersTotal:   counter("ledger_transfers_total", "Transfer attempts by outcome.", "outcome", "reason"),
		ReversalsTotal:   counter("ledger_reversals_total", "Reversal attempts by outcome.", "outcome", "reason"),
		IdempotencyTotal: counter("ledger_idempotency_total", "Idempotency key outcomes.", "result", "source"),

		PostgresRetries: counter("ledger_postgres_retries_total", "PostgreSQL transaction retries.", "reason"),
		PostgresTxTotal: counter("ledger_postgres_transactions_total", "PostgreSQL transactions by outcome.", "outcome"),

		RedisOps: counter("ledger_redis_operations_total", "Redis operations by result.", "op", "result"),

		KafkaPublishTotal: counter("ledger_kafka_publish_total", "Kafka publish attempts.", "topic", "outcome"),
		KafkaRetryTotal:   counter("ledger_kafka_retry_total", "Kafka publish retries.", "topic"),
		KafkaConsumeTotal: counter("ledger_kafka_consume_total", "Kafka messages consumed.", "topic", "outcome"),

		OutboxBacklog:     gauge("ledger_outbox_backlog", "Outbox rows awaiting publication."),
		OutboxDeadLetters: plainCounter("ledger_outbox_dead_letter_total", "Outbox rows moved to dead letter."),

		WebhookDeliveries: counter("ledger_webhook_deliveries_total", "Webhook delivery outcomes.", "outcome"),
		WebhookDuplicates: plainCounter("ledger_webhook_duplicate_total", "Duplicate webhook events suppressed."),

		ReplayEvents: counter("ledger_replay_events_total", "Replayed events by outcome.", "outcome"),

		ReconciliationMismatches: plainCounter("ledger_reconciliation_mismatch_total", "Accounts whose balance disagreed with their entries."),
		ReconciliationRuns:       counter("ledger_reconciliation_runs_total", "Reconciliation runs by outcome.", "outcome"),

		RateLimited: counter("ledger_rate_limited_total", "Requests rejected by the rate limiter.", "route"),
	}
}

// Handler exposes /metrics for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the underlying registry (used by tests).
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }
