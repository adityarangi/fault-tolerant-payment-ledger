// Command webhook-worker consumes payment events and delivers them to
// configured webhook endpoints with durable retries.
package main

import (
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/adityarangi/payment-ledger/internal/app"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/webhook"
)

func main() {
	ctx, cancel := app.SignalContext()
	defer cancel()

	deps, err := app.Build(ctx, "webhook-worker")
	if err != nil {
		panic(err)
	}
	defer deps.Close()

	ctx = observability.WithLogger(ctx, deps.Logger)

	topics := []string{
		deps.Config.TopicPaymentsCreated,
		deps.Config.TopicPaymentsReversed,
		deps.Config.TopicPaymentsFailed,
	}
	if err := kafka.EnsureTopics(ctx, deps.Config.KafkaBrokers, 3, deps.Config.AllTopics()...); err != nil {
		deps.Logger.Warn("could not ensure kafka topics", "error", err.Error())
	}

	worker := webhook.NewWorker(deps.DB.Pool(), deps.Config, deps.Metrics, deps.Failpoints)
	consumer := kafka.NewConsumer(deps.Config, deps.Config.WebhookConsumerGroup, topics, deps.Metrics)
	defer func() {
		_ = consumer.Close()
	}()

	go serveMetrics(deps)

	var wg sync.WaitGroup
	wg.Add(2)

	// The consumer only records intent; the delivery loop performs the HTTP
	// calls. Any deliveries left pending by a previous process are picked up
	// automatically by the loop on startup.
	go func() {
		defer wg.Done()
		if err := consumer.Run(ctx, worker.HandleEvent); err != nil {
			deps.Logger.Error("kafka consumer failed", "error", err.Error())
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		if err := worker.RunDeliveries(ctx); err != nil {
			deps.Logger.Error("webhook delivery loop failed", "error", err.Error())
			cancel()
		}
	}()

	wg.Wait()
	deps.Logger.Info("webhook worker stopped")
}

func serveMetrics(deps *app.Deps) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", deps.Metrics.Handler())
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	addr := os.Getenv("LEDGER_METRICS_ADDR")
	if addr == "" {
		addr = ":8082"
	}
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		deps.Logger.Warn("metrics server stopped", "error", err.Error())
	}
}
