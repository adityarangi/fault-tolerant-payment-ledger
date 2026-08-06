//go:build integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/kafka"
)

// TestConsumerSurvivesBrokerOutage is a regression test for a real bug: the
// webhook consumer used to return an error the first time a fetch failed,
// which made the worker exit permanently after a transient Kafka restart. It
// must instead keep retrying until the context is cancelled.
func TestConsumerSurvivesBrokerOutage(t *testing.T) {
	h := newHarness(t, func(cfg *config.Config) {
		// Nothing is listening on this port, so every fetch fails.
		cfg.KafkaBrokers = []string{"127.0.0.1:9099"}
	})

	consumer := kafka.NewConsumer(h.cfg, "resilience-test", []string{h.cfg.TopicPaymentsCreated}, h.metrics)
	t.Cleanup(func() { _ = consumer.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, func(context.Context, kafka.Message) error { return nil })
	}()

	// Give it long enough to fail several fetches against a dead broker.
	select {
	case err := <-done:
		t.Fatalf("consumer exited on a broker outage instead of retrying: %v", err)
	case <-time.After(3 * time.Second):
		// Still running, which is the point.
	}

	// It must still shut down promptly when actually asked to.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consumer returned an error on shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer did not stop after its context was cancelled")
	}
}
