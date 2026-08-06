// Package app wires the shared dependency graph used by every binary, so the
// API, the workers and the CLIs all agree on configuration and observability.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/storage"
)

// Deps is the shared dependency set.
type Deps struct {
	Config     *config.Config
	Logger     *slog.Logger
	Metrics    *observability.Metrics
	DB         *storage.DB
	Redis      *redis.Client
	Failpoints *failpoint.Registry
}

// Build constructs the shared dependencies for a service.
//
// Redis is intentionally optional: if it cannot be reached the process starts
// anyway with caching and rate limiting disabled, because neither is required
// for the ledger to be correct.
func Build(ctx context.Context, serviceName string) (*Deps, error) {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return nil, err
	}

	logger := observability.NewLogger(cfg.ServiceName, cfg.LogLevel)
	slog.SetDefault(logger)
	metrics := observability.NewMetrics()

	fp := failpoint.NewRegistry(cfg.FailpointsEnabled)
	if err := fp.Parse(cfg.Failpoints); err != nil {
		return nil, err
	}
	if fp.Enabled() {
		logger.Warn("failpoint injection is ENABLED; this must never be set in production",
			"active", fp.Active())
	}

	db, err := storage.Open(ctx, cfg, metrics)
	if err != nil {
		return nil, fmt.Errorf("app: postgres: %w", err)
	}

	var rdb *redis.Client
	if cfg.RedisEnabled {
		rdb = redis.NewClient(&redis.Options{
			Addr:         cfg.RedisAddr,
			Password:     cfg.RedisPassword,
			DB:           cfg.RedisDB,
			DialTimeout:  cfg.RedisTimeout,
			ReadTimeout:  cfg.RedisTimeout,
			WriteTimeout: cfg.RedisTimeout,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis unavailable at startup; continuing without cache or rate limiting",
				"addr", cfg.RedisAddr, "error", err.Error())
		}
	}

	return &Deps{
		Config:     cfg,
		Logger:     logger,
		Metrics:    metrics,
		DB:         db,
		Redis:      rdb,
		Failpoints: fp,
	}, nil
}

// Close releases the shared dependencies.
func (d *Deps) Close() {
	if d.DB != nil {
		d.DB.Close()
	}
	if d.Redis != nil {
		_ = d.Redis.Close()
	}
}

// SignalContext returns a context cancelled on SIGINT/SIGTERM, giving every
// worker a clean shutdown path.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
