// Command outbox-worker drains the transactional outbox to Kafka.
//
// Multiple replicas may run concurrently: rows are claimed with
// FOR UPDATE SKIP LOCKED, so workers never collide and never block each other.
package main

import (
	"net/http"
	"os"
	"time"

	"github.com/adityarangi/payment-ledger/internal/app"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/outbox"
)

func main() {
	ctx, cancel := app.SignalContext()
	defer cancel()

	deps, err := app.Build(ctx, "outbox-worker")
	if err != nil {
		panic(err)
	}
	defer deps.Close()

	if err := kafka.EnsureTopics(ctx, deps.Config.KafkaBrokers, 3, deps.Config.AllTopics()...); err != nil {
		// Topic creation is best effort; brokers may create them on demand.
		deps.Logger.Warn("could not ensure kafka topics", "error", err.Error())
	}

	producer := kafka.NewProducer(deps.Config, deps.Metrics)
	defer func() {
		_ = producer.Close()
	}()

	// Expose metrics so the outbox backlog gauge is scrapeable.
	go serveMetrics(deps)

	publisher := outbox.NewPublisher(deps.DB.Pool(), producer, deps.Config, deps.Metrics, deps.Failpoints)
	if err := publisher.Run(ctx); err != nil {
		deps.Logger.Error("outbox worker failed", "error", err.Error())
		os.Exit(1)
	}
}

func serveMetrics(deps *app.Deps) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", deps.Metrics.Handler())
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	server := &http.Server{
		Addr:              metricsAddr(),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		deps.Logger.Warn("metrics server stopped", "error", err.Error())
	}
}

func metricsAddr() string {
	if addr := os.Getenv("LEDGER_METRICS_ADDR"); addr != "" {
		return addr
	}
	return ":8081"
}
