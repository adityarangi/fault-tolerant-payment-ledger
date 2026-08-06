// Command api serves the ledger HTTP API.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/adityarangi/payment-ledger/internal/api"
	"github.com/adityarangi/payment-ledger/internal/app"
	"github.com/adityarangi/payment-ledger/internal/idempotency"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/ledger"
	"github.com/adityarangi/payment-ledger/internal/outbox"
	"github.com/adityarangi/payment-ledger/internal/reconciliation"
)

func main() {
	ctx, cancel := app.SignalContext()
	defer cancel()

	deps, err := app.Build(ctx, "ledger-api")
	if err != nil {
		panic(err)
	}
	defer deps.Close()

	producer := kafka.NewProducer(deps.Config, deps.Metrics)
	defer func() {
		_ = producer.Close()
	}()

	server := api.NewServer(api.Options{
		Config:     deps.Config,
		DB:         deps.DB,
		Ledger:     ledger.NewService(deps.DB, deps.Config, deps.Metrics, deps.Failpoints),
		Cache:      idempotency.NewCache(deps.Redis, deps.Config.IdempotencyCacheTTL, deps.Config.RedisTimeout, deps.Metrics),
		Redis:      deps.Redis,
		Replayer:   outbox.NewReplayer(deps.DB.Pool(), producer, deps.Config, deps.Metrics),
		Reconciler: reconciliation.New(deps.DB.Pool(), deps.Metrics),
		Producer:   producer,
		Metrics:    deps.Metrics,
		Failpoints: deps.Failpoints,
		Logger:     deps.Logger,
	})

	httpServer := &http.Server{
		Addr:              deps.Config.HTTPAddr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		deps.Logger.Info("shutting down api")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			deps.Logger.Error("graceful shutdown failed", "error", err.Error())
		}
	}()

	deps.Logger.Info("ledger api listening", "addr", deps.Config.HTTPAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		deps.Logger.Error("http server failed", "error", err.Error())
		os.Exit(1)
	}
	deps.Logger.Info("api stopped")
}
