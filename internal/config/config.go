// Package config loads process configuration from the environment.
//
// Every value has a development-friendly default so that `docker compose up`
// works with no .env file, but nothing secret is ever defaulted.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full configuration surface shared by all binaries.
type Config struct {
	ServiceName string
	LogLevel    string
	HTTPAddr    string

	DatabaseURL     string
	DBMaxConns      int32
	DBMinConns      int32
	DBLockTimeout   time.Duration
	DBStatementTime time.Duration
	DBMaxTxRetries  int

	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisEnabled  bool
	RedisTimeout  time.Duration

	IdempotencyCacheTTL time.Duration

	RateLimitEnabled bool
	RateLimitRPS     int
	RateLimitBurst   int

	KafkaBrokers  []string
	KafkaEnabled  bool
	KafkaClientID string
	KafkaTimeout  time.Duration

	TopicPaymentsCreated  string
	TopicPaymentsReversed string
	TopicPaymentsFailed   string
	TopicPaymentsDLQ      string

	OutboxBatchSize    int
	OutboxPollInterval time.Duration
	OutboxMaxAttempts  int
	OutboxBaseBackoff  time.Duration
	OutboxMaxBackoff   time.Duration
	OutboxClaimTTL     time.Duration
	OutboxWorkerID     string

	WebhookEndpoints     []string
	WebhookConsumerGroup string
	WebhookMaxAttempts   int
	WebhookBaseBackoff   time.Duration
	WebhookMaxBackoff    time.Duration
	WebhookTimeout       time.Duration
	WebhookPollInterval  time.Duration
	WebhookBatchSize     int
	WebhookSigningSecret string

	FailpointsEnabled bool
	Failpoints        string
}

// Load reads configuration from the environment.
func Load(serviceName string) (*Config, error) {
	cfg := &Config{
		ServiceName: envStr("LEDGER_SERVICE_NAME", serviceName),
		LogLevel:    envStr("LEDGER_LOG_LEVEL", "info"),
		HTTPAddr:    envStr("LEDGER_HTTP_ADDR", ":8080"),

		DatabaseURL:     envStr("LEDGER_DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
		DBMaxConns:      int32(envInt("LEDGER_DB_MAX_CONNS", 20)),
		DBMinConns:      int32(envInt("LEDGER_DB_MIN_CONNS", 2)),
		DBLockTimeout:   envDur("LEDGER_DB_LOCK_TIMEOUT", 3*time.Second),
		DBStatementTime: envDur("LEDGER_DB_STATEMENT_TIMEOUT", 10*time.Second),
		DBMaxTxRetries:  envInt("LEDGER_DB_MAX_TX_RETRIES", 5),

		RedisAddr:     envStr("LEDGER_REDIS_ADDR", "localhost:6379"),
		RedisPassword: os.Getenv("LEDGER_REDIS_PASSWORD"),
		RedisDB:       envInt("LEDGER_REDIS_DB", 0),
		RedisEnabled:  envBool("LEDGER_REDIS_ENABLED", true),
		RedisTimeout:  envDur("LEDGER_REDIS_TIMEOUT", 150*time.Millisecond),

		IdempotencyCacheTTL: envDur("LEDGER_IDEMPOTENCY_CACHE_TTL", 24*time.Hour),

		RateLimitEnabled: envBool("LEDGER_RATE_LIMIT_ENABLED", true),
		RateLimitRPS:     envInt("LEDGER_RATE_LIMIT_RPS", 200),
		RateLimitBurst:   envInt("LEDGER_RATE_LIMIT_BURST", 400),

		KafkaBrokers:  envList("LEDGER_KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaEnabled:  envBool("LEDGER_KAFKA_ENABLED", true),
		KafkaClientID: envStr("LEDGER_KAFKA_CLIENT_ID", serviceName),
		KafkaTimeout:  envDur("LEDGER_KAFKA_TIMEOUT", 5*time.Second),

		TopicPaymentsCreated:  envStr("LEDGER_TOPIC_PAYMENTS_CREATED", "payments.created"),
		TopicPaymentsReversed: envStr("LEDGER_TOPIC_PAYMENTS_REVERSED", "payments.reversed"),
		TopicPaymentsFailed:   envStr("LEDGER_TOPIC_PAYMENTS_FAILED", "payments.failed"),
		TopicPaymentsDLQ:      envStr("LEDGER_TOPIC_PAYMENTS_DLQ", "payments.dlq"),

		OutboxBatchSize:    envInt("LEDGER_OUTBOX_BATCH_SIZE", 100),
		OutboxPollInterval: envDur("LEDGER_OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxMaxAttempts:  envInt("LEDGER_OUTBOX_MAX_ATTEMPTS", 8),
		OutboxBaseBackoff:  envDur("LEDGER_OUTBOX_BASE_BACKOFF", 250*time.Millisecond),
		OutboxMaxBackoff:   envDur("LEDGER_OUTBOX_MAX_BACKOFF", 60*time.Second),
		OutboxClaimTTL:     envDur("LEDGER_OUTBOX_CLAIM_TTL", 30*time.Second),
		OutboxWorkerID:     envStr("LEDGER_OUTBOX_WORKER_ID", defaultWorkerID()),

		WebhookEndpoints:     envList("LEDGER_WEBHOOK_ENDPOINTS", []string{"http://localhost:9090/webhook"}),
		WebhookConsumerGroup: envStr("LEDGER_WEBHOOK_CONSUMER_GROUP", "webhook-worker"),
		WebhookMaxAttempts:   envInt("LEDGER_WEBHOOK_MAX_ATTEMPTS", 5),
		WebhookBaseBackoff:   envDur("LEDGER_WEBHOOK_BASE_BACKOFF", 500*time.Millisecond),
		WebhookMaxBackoff:    envDur("LEDGER_WEBHOOK_MAX_BACKOFF", 5*time.Minute),
		WebhookTimeout:       envDur("LEDGER_WEBHOOK_TIMEOUT", 5*time.Second),
		WebhookPollInterval:  envDur("LEDGER_WEBHOOK_POLL_INTERVAL", 500*time.Millisecond),
		WebhookBatchSize:     envInt("LEDGER_WEBHOOK_BATCH_SIZE", 50),
		WebhookSigningSecret: os.Getenv("LEDGER_WEBHOOK_SIGNING_SECRET"),

		FailpointsEnabled: envBool("LEDGER_FAILPOINTS_ENABLED", false),
		Failpoints:        os.Getenv("LEDGER_FAILPOINTS"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("config: LEDGER_DATABASE_URL is required")
	}
	if cfg.DBMaxTxRetries < 1 {
		return nil, fmt.Errorf("config: LEDGER_DB_MAX_TX_RETRIES must be >= 1")
	}
	if cfg.OutboxMaxAttempts < 1 {
		return nil, fmt.Errorf("config: LEDGER_OUTBOX_MAX_ATTEMPTS must be >= 1")
	}
	if cfg.WebhookMaxAttempts < 1 {
		return nil, fmt.Errorf("config: LEDGER_WEBHOOK_MAX_ATTEMPTS must be >= 1")
	}
	return cfg, nil
}

// TopicForEvent maps an event type to its Kafka topic.
func (c *Config) TopicForEvent(eventType string) string {
	switch eventType {
	case "payment.reversed":
		return c.TopicPaymentsReversed
	case "payment.failed":
		return c.TopicPaymentsFailed
	default:
		return c.TopicPaymentsCreated
	}
}

// AllTopics lists every topic the system uses.
func (c *Config) AllTopics() []string {
	return []string{c.TopicPaymentsCreated, c.TopicPaymentsReversed, c.TopicPaymentsFailed, c.TopicPaymentsDLQ}
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envList(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}
