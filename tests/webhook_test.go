//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/ledger"
	"github.com/adityarangi/payment-ledger/internal/outbox"
	"github.com/adityarangi/payment-ledger/internal/webhook"
)

// recordingEndpoint is a test webhook receiver that records the event IDs it
// has seen and can be told to fail a number of times.
type recordingEndpoint struct {
	server *httptest.Server

	mu         sync.Mutex
	deliveries map[string]int

	failuresLeft atomic.Int32
	failAlways   atomic.Bool
	totalCalls   atomic.Int32
}

func newRecordingEndpoint(t *testing.T) *recordingEndpoint {
	t.Helper()
	e := &recordingEndpoint{deliveries: map[string]int{}}
	e.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.totalCalls.Add(1)

		if e.failAlways.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if e.failuresLeft.Load() > 0 {
			e.failuresLeft.Add(-1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		var env outbox.Envelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		e.mu.Lock()
		e.deliveries[env.EventID]++
		e.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(e.server.Close)
	return e
}

func (e *recordingEndpoint) url() string { return e.server.URL }

func (e *recordingEndpoint) uniqueEvents() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.deliveries)
}

func (e *recordingEndpoint) deliveriesFor(eventID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.deliveries[eventID]
}

// makeEvent produces a real payment event and returns its envelope.
func (h *harness) makeEvent(t *testing.T) (outbox.Envelope, ledger.Transaction) {
	t.Helper()
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	var txn ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 2_500, "USD").decode(t, &txn)

	records, err := outbox.ListForReplay(ctx, h.db.Pool(), txn.ID, nil, nil, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("load outbox event: %v (%d records)", err, len(records))
	}
	var env outbox.Envelope
	if err := json.Unmarshal(records[0].Payload, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return env, txn
}

// TestWebhookDeliverySucceeds is the happy path from Kafka message to HTTP
// delivery.
func TestWebhookDeliverySucceeds(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
	})
	ctx := context.Background()

	env, _ := h.makeEvent(t)
	worker := h.newWebhookWorker()

	payload, _ := json.Marshal(env)
	if err := worker.HandleEvent(ctx, kafka.Message{
		Topic: h.cfg.TopicPaymentsCreated,
		Key:   env.TransactionID,
		Value: payload,
	}); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	eventually(t, 10*time.Second, "webhook to be delivered", func() bool {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Logf("deliver: %v", err)
		}
		return endpoint.deliveriesFor(env.EventID) == 1
	})

	delivery, err := webhook.Get(ctx, h.db.Pool(), env.EventID, endpoint.url())
	if err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if delivery.Status != webhook.StatusDelivered {
		t.Fatalf("delivery status = %q, want delivered", delivery.Status)
	}
}

// TestWebhookTemporaryFailureAndRetry proves a transient endpoint failure is
// retried with backoff and eventually succeeds.
func TestWebhookTemporaryFailureAndRetry(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	endpoint.failuresLeft.Store(2) // fail twice, then accept

	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
		cfg.WebhookMaxAttempts = 5
	})
	ctx := context.Background()

	env, _ := h.makeEvent(t)
	worker := h.newWebhookWorker()
	payload, _ := json.Marshal(env)
	if err := worker.HandleEvent(ctx, kafka.Message{Value: payload}); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	eventually(t, 20*time.Second, "webhook to succeed after retries", func() bool {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Logf("deliver: %v", err)
		}
		return endpoint.deliveriesFor(env.EventID) == 1
	})

	delivery, err := webhook.Get(ctx, h.db.Pool(), env.EventID, endpoint.url())
	if err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if delivery.Status != webhook.StatusDelivered {
		t.Fatalf("status = %q, want delivered", delivery.Status)
	}
	if delivery.Attempts < 3 {
		t.Fatalf("attempts = %d, want at least 3 (two failures then a success)", delivery.Attempts)
	}
	h.requireReconciled(ctx)
}

// TestWebhookDeadLetter proves an endpoint that never recovers exhausts its
// attempt budget and stops, rather than retrying forever.
func TestWebhookDeadLetter(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	endpoint.failAlways.Store(true)

	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
		cfg.WebhookMaxAttempts = 3
	})
	ctx := context.Background()

	env, _ := h.makeEvent(t)
	worker := h.newWebhookWorker()
	payload, _ := json.Marshal(env)
	if err := worker.HandleEvent(ctx, kafka.Message{Value: payload}); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	eventually(t, 20*time.Second, "webhook to be dead-lettered", func() bool {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Logf("deliver: %v", err)
		}
		d, err := webhook.Get(ctx, h.db.Pool(), env.EventID, endpoint.url())
		return err == nil && d.Status == webhook.StatusDeadLetter
	})

	delivery, err := webhook.Get(ctx, h.db.Pool(), env.EventID, endpoint.url())
	if err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if delivery.Attempts != 3 {
		t.Fatalf("attempts = %d, want exactly the 3-attempt budget", delivery.Attempts)
	}
	if delivery.LastError == "" {
		t.Fatal("last_error was not recorded")
	}

	// Further cycles must not keep hammering a dead-lettered endpoint.
	callsAtDeadLetter := endpoint.totalCalls.Load()
	for i := 0; i < 3; i++ {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	if got := endpoint.totalCalls.Load(); got != callsAtDeadLetter {
		t.Fatalf("dead-lettered delivery was retried %d more times", got-callsAtDeadLetter)
	}
	h.requireReconciled(ctx)
}

// TestDuplicateKafkaDeliveryIsIdempotent feeds the same event several times,
// as an at-least-once broker would. Exactly one delivery row and one outbound
// call must result (INVARIANT 11).
func TestDuplicateKafkaDeliveryIsIdempotent(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
	})
	ctx := context.Background()

	env, _ := h.makeEvent(t)
	worker := h.newWebhookWorker()
	payload, _ := json.Marshal(env)

	// The broker redelivers the same event five times.
	for i := 0; i < 5; i++ {
		if err := worker.HandleEvent(ctx, kafka.Message{Value: payload}); err != nil {
			t.Fatalf("handle event %d: %v", i, err)
		}
	}

	var rows int
	if err := h.db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM webhook_deliveries WHERE event_id = $1`, env.EventID).Scan(&rows); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if rows != 1 {
		t.Fatalf("INVARIANT 11 violated: %d delivery rows for one event, want 1", rows)
	}

	eventually(t, 10*time.Second, "webhook delivery", func() bool {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Logf("deliver: %v", err)
		}
		return endpoint.deliveriesFor(env.EventID) >= 1
	})

	// Extra cycles must not produce extra outbound calls.
	for i := 0; i < 3; i++ {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	if got := endpoint.deliveriesFor(env.EventID); got != 1 {
		t.Fatalf("endpoint was called %d times for one event, want 1", got)
	}
}

// TestReplayedEventDoesNotDuplicateWebhookState proves a replayed event, which
// keeps its original event ID, is recognised as a duplicate.
func TestReplayedEventDoesNotDuplicateWebhookState(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
	})
	ctx := context.Background()

	env, _ := h.makeEvent(t)
	worker := h.newWebhookWorker()
	payload, _ := json.Marshal(env)
	if err := worker.HandleEvent(ctx, kafka.Message{Value: payload}); err != nil {
		t.Fatalf("handle event: %v", err)
	}
	eventually(t, 10*time.Second, "initial delivery", func() bool {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Logf("deliver: %v", err)
		}
		return endpoint.deliveriesFor(env.EventID) == 1
	})

	// Now the same event arrives again, flagged as a replay.
	replayed := env
	replayed.Replay = &outbox.ReplayMeta{
		IsReplay:   true,
		ReplayID:   uuid.NewString(),
		ReplayedAt: time.Now().UTC(),
	}
	replayedPayload, _ := json.Marshal(replayed)
	if err := worker.HandleEvent(ctx, kafka.Message{Value: replayedPayload}); err != nil {
		t.Fatalf("handle replayed event: %v", err)
	}

	var rows int
	if err := h.db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM webhook_deliveries WHERE event_id = $1`, env.EventID).Scan(&rows); err != nil {
		t.Fatalf("count deliveries: %v", err)
	}
	if rows != 1 {
		t.Fatalf("replay created %d delivery rows, want 1", rows)
	}
	for i := 0; i < 3; i++ {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Fatalf("deliver: %v", err)
		}
	}
	if got := endpoint.deliveriesFor(env.EventID); got != 1 {
		t.Fatalf("endpoint received %d calls after a replay, want 1", got)
	}
	if endpoint.uniqueEvents() != 1 {
		t.Fatalf("endpoint saw %d unique events, want 1", endpoint.uniqueEvents())
	}
}

// TestWebhookRecoversPendingDeliveriesAfterRestart proves delivery state lives
// in PostgreSQL, not in worker memory.
func TestWebhookRecoversPendingDeliveriesAfterRestart(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
	})
	ctx := context.Background()

	env, _ := h.makeEvent(t)

	// The first worker records the delivery, then "crashes" before sending.
	first := h.newWebhookWorker()
	payload, _ := json.Marshal(env)
	if err := first.HandleEvent(ctx, kafka.Message{Value: payload}); err != nil {
		t.Fatalf("handle event: %v", err)
	}
	delivery, err := webhook.Get(ctx, h.db.Pool(), env.EventID, endpoint.url())
	if err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if delivery.Status != webhook.StatusPending {
		t.Fatalf("status = %q, want pending", delivery.Status)
	}
	if endpoint.totalCalls.Load() != 0 {
		t.Fatal("endpoint was called before the delivery loop ran")
	}

	// A brand new worker process picks the pending row up.
	restarted := h.newWebhookWorker()
	eventually(t, 10*time.Second, "restarted worker to deliver", func() bool {
		if _, err := restarted.DeliverOnce(ctx); err != nil {
			t.Logf("deliver: %v", err)
		}
		return endpoint.deliveriesFor(env.EventID) == 1
	})

	final, err := webhook.Get(ctx, h.db.Pool(), env.EventID, endpoint.url())
	if err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if final.Status != webhook.StatusDelivered {
		t.Fatalf("status = %q, want delivered", final.Status)
	}
}

// TestWebhookPersistFailpointRedelivers proves that failing before the
// delivery row is persisted leaves no state and returns an error, so Kafka
// redelivers the event.
func TestWebhookPersistFailpointRedelivers(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
	})
	ctx := context.Background()

	env, _ := h.makeEvent(t)
	worker := h.newWebhookWorker()
	payload, _ := json.Marshal(env)

	h.failpoints.Arm(failpoint.BeforeWebhookPersist, "error:1")
	if err := worker.HandleEvent(ctx, kafka.Message{Value: payload}); err == nil {
		t.Fatal("expected an error so the Kafka offset is not committed")
	}

	counts, err := webhook.CountByStatus(ctx, h.db.Pool())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("delivery state was persisted despite the failure: %v", counts)
	}

	// Kafka redelivers; this time it works.
	if err := worker.HandleEvent(ctx, kafka.Message{Value: payload}); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	eventually(t, 10*time.Second, "delivery after redelivery", func() bool {
		if _, err := worker.DeliverOnce(ctx); err != nil {
			t.Logf("deliver: %v", err)
		}
		return endpoint.deliveriesFor(env.EventID) == 1
	})
}

// TestWebhookMalformedEventIsDiscarded proves a poison message is dropped
// rather than blocking the consumer group forever.
func TestWebhookMalformedEventIsDiscarded(t *testing.T) {
	endpoint := newRecordingEndpoint(t)
	h := newHarness(t, func(cfg *config.Config) {
		cfg.WebhookEndpoints = []string{endpoint.url()}
	})
	ctx := context.Background()
	worker := h.newWebhookWorker()

	if err := worker.HandleEvent(ctx, kafka.Message{Value: []byte("not json")}); err != nil {
		t.Fatalf("malformed message should be discarded, got: %v", err)
	}
	if err := worker.HandleEvent(ctx, kafka.Message{Value: []byte(`{"event_type":"x"}`)}); err != nil {
		t.Fatalf("message without event_id should be discarded, got: %v", err)
	}

	counts, err := webhook.CountByStatus(ctx, h.db.Pool())
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("malformed messages created delivery state: %v", counts)
	}
}
