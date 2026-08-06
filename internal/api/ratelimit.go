package api

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adityarangi/payment-ledger/internal/observability"
)

// RateLimiter is a Redis-backed fixed-window limiter shared across replicas.
//
// It deliberately FAILS OPEN. Redis is an optimisation here; losing it should
// degrade throttling, not availability, and it can never affect ledger
// correctness because it sits entirely outside the payment transaction.
type RateLimiter struct {
	client  *redis.Client
	limit   int
	window  time.Duration
	timeout time.Duration
	metrics *observability.Metrics
}

// NewRateLimiter builds a limiter allowing `limit` requests per window.
func NewRateLimiter(client *redis.Client, limit int, window, timeout time.Duration, metrics *observability.Metrics) *RateLimiter {
	if client == nil || limit <= 0 {
		return nil
	}
	return &RateLimiter{client: client, limit: limit, window: window, timeout: timeout, metrics: metrics}
}

// Allow reports whether the caller may proceed, and how long to wait if not.
func (l *RateLimiter) Allow(ctx context.Context, key string) (bool, time.Duration) {
	if l == nil {
		return true, 0
	}
	opCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	window := time.Now().UnixNano() / int64(l.window)
	redisKey := "ledger:rl:" + key + ":" + time.Duration(window).String()

	pipe := l.client.TxPipeline()
	incr := pipe.Incr(opCtx, redisKey)
	pipe.Expire(opCtx, redisKey, l.window*2)
	if _, err := pipe.Exec(opCtx); err != nil {
		l.metrics.RedisOps.WithLabelValues("ratelimit", "error").Inc()
		observability.Logger(ctx).Warn("rate limiter unavailable, failing open", "error", err.Error())
		return true, 0
	}

	count := incr.Val()
	if count > int64(l.limit) {
		l.metrics.RedisOps.WithLabelValues("ratelimit", "hit").Inc()
		return false, l.window
	}
	l.metrics.RedisOps.WithLabelValues("ratelimit", "miss").Inc()
	return true, 0
}
