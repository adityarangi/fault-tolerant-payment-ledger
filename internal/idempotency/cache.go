package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adityarangi/payment-ledger/internal/observability"
)

// CachedResponse is the response replayed on an idempotent retry.
type CachedResponse struct {
	Status        int             `json:"status"`
	Body          json.RawMessage `json:"body"`
	RequestHash   string          `json:"request_hash"`
	TransactionID string          `json:"transaction_id,omitempty"`
}

// Cache is a best-effort Redis cache of completed idempotency responses.
//
// It is deliberately non-authoritative. Every method degrades to "miss" on any
// error or timeout, and a miss always falls through to PostgreSQL. Flushing or
// losing Redis entirely cannot change any financial outcome; it can only make
// retries slower (INVARIANT 12).
type Cache struct {
	client  *redis.Client
	ttl     time.Duration
	timeout time.Duration
	metrics *observability.Metrics
	enabled bool
}

// NewCache builds a cache. A nil client yields a disabled cache whose methods
// are no-ops.
func NewCache(client *redis.Client, ttl, timeout time.Duration, metrics *observability.Metrics) *Cache {
	return &Cache{
		client:  client,
		ttl:     ttl,
		timeout: timeout,
		metrics: metrics,
		enabled: client != nil,
	}
}

func cacheKey(scope, key string) string { return "ledger:idem:" + scope + ":" + key }

// Get returns the cached response for a key, or nil on any miss or failure.
func (c *Cache) Get(ctx context.Context, scope, key string) *CachedResponse {
	if c == nil || !c.enabled {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	raw, err := c.client.Get(opCtx, cacheKey(scope, key)).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		c.metrics.RedisOps.WithLabelValues("idem_get", "miss").Inc()
		return nil
	case err != nil:
		// A Redis outage is not an API error: fall through to PostgreSQL.
		c.metrics.RedisOps.WithLabelValues("idem_get", "error").Inc()
		observability.Logger(ctx).Warn("redis idempotency cache read failed, falling back to postgres",
			"error", err.Error())
		return nil
	}

	var resp CachedResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		c.metrics.RedisOps.WithLabelValues("idem_get", "error").Inc()
		return nil
	}
	c.metrics.RedisOps.WithLabelValues("idem_get", "hit").Inc()
	return &resp
}

// Put stores a completed response. Failures are logged and ignored.
func (c *Cache) Put(ctx context.Context, scope, key string, resp CachedResponse) {
	if c == nil || !c.enabled {
		return
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if err := c.client.Set(opCtx, cacheKey(scope, key), raw, c.ttl).Err(); err != nil {
		c.metrics.RedisOps.WithLabelValues("idem_put", "error").Inc()
		observability.Logger(ctx).Warn("redis idempotency cache write failed", "error", err.Error())
		return
	}
	c.metrics.RedisOps.WithLabelValues("idem_put", "ok").Inc()
}
