//go:build integration

package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/ledger"
	"github.com/adityarangi/payment-ledger/internal/outbox"
)

// TestOutboxWrittenInSameTransaction proves the event and the payment are
// atomic with each other: the outbox row exists the moment the transfer
// returns, with no worker involved.
func TestOutboxWrittenInSameTransaction(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	var txn ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 2_500, "USD").decode(t, &txn)

	records, err := outbox.ListForReplay(ctx, h.db.Pool(), txn.ID, nil, nil, 10)
	if err != nil {
		t.Fatalf("list outbox: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d outbox rows for the transaction, want 1", len(records))
	}
	rec := records[0]
	if rec.Status != outbox.StatusPending {
		t.Fatalf("outbox status = %q, want pending", rec.Status)
	}
	if rec.EventType != ledger.EventPaymentCreated {
		t.Fatalf("event type = %q, want %q", rec.EventType, ledger.EventPaymentCreated)
	}

	var env outbox.Envelope
	if err := json.Unmarshal(rec.Payload, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.EventID != rec.ID {
		t.Fatalf("envelope event_id %q does not match outbox row id %q", env.EventID, rec.ID)
	}
	if env.TransactionID != txn.ID {
		t.Fatalf("envelope transaction_id = %q, want %q", env.TransactionID, txn.ID)
	}
}

// TestOutboxPublishesToKafka runs the publisher and checks the row reaches the
// published state.
func TestOutboxPublishesToKafka(t *testing.T) {
	h := newHarness(t)
	if !kafkaAvailable(h.cfg) {
		t.Skip("kafka not reachable")
	}
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")
	h.transfer(uuid.NewString(), a, b, 2_500, "USD")

	publisher := h.newPublisher()
	eventually(t, 30*time.Second, "outbox to drain", func() bool {
		if _, err := publisher.RunOnce(ctx); err != nil {
			t.Logf("publish cycle: %v", err)
		}
		return h.countOutbox(ctx, outbox.StatusPending) == 0 &&
			h.countOutbox(ctx, outbox.StatusPublishing) == 0
	})

	if got := h.countOutbox(ctx, outbox.StatusPublished); got == 0 {
		t.Fatal("no outbox rows reached the published state")
	}
}

// TestKafkaUnavailableDoesNotBlockPayments proves the ledger keeps accepting
// payments while Kafka is down: events simply queue in PostgreSQL.
func TestKafkaUnavailableDoesNotBlockPayments(t *testing.T) {
	h := newHarness(t, func(cfg *config.Config) {
		// Nothing is listening here.
		cfg.KafkaBrokers = []string{"127.0.0.1:9099"}
		cfg.KafkaTimeout = time.Second
	})
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	resp := h.transfer(uuid.NewString(), a, b, 2_500, "USD")
	if resp.Status != http.StatusCreated {
		t.Fatalf("transfer with Kafka down: status %d body %s", resp.Status, resp.Body)
	}
	if got := h.balance(b); got != 2_500 {
		t.Fatalf("balance = %d, want 2500; Kafka must not gate payments", got)
	}
	if h.countOutbox(ctx, outbox.StatusPending) == 0 {
		t.Fatal("expected the event to be queued in the outbox")
	}
	h.requireReconciled(ctx)
}

// TestOutboxRetryAfterKafkaFailure checks bounded retries, recorded attempts
// and last_error, and that none of it touches the ledger (INVARIANT 10).
func TestOutboxRetryAfterKafkaFailure(t *testing.T) {
	h := newHarness(t, func(cfg *config.Config) {
		cfg.KafkaBrokers = []string{"127.0.0.1:9099"}
		cfg.KafkaTimeout = 500 * time.Millisecond
		cfg.OutboxMaxAttempts = 3
	})
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")
	h.transfer(uuid.NewString(), a, b, 2_500, "USD")

	balanceBefore := h.balance(a)
	entriesBefore := h.countEntries(ctx)

	publisher := h.newPublisher()

	// Attempt 1 fails and schedules a retry.
	if _, err := publisher.RunOnce(ctx); err != nil {
		t.Fatalf("publish cycle: %v", err)
	}
	var attempts int
	var lastError *string
	if err := h.db.Pool().QueryRow(ctx,
		`SELECT attempts, last_error FROM outbox_events LIMIT 1`).Scan(&attempts, &lastError); err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if lastError == nil || *lastError == "" {
		t.Fatal("last_error was not recorded")
	}

	// Exhaust the budget; the row must end up dead-lettered, not retried
	// forever.
	eventually(t, 30*time.Second, "outbox row to be dead-lettered", func() bool {
		if _, err := publisher.RunOnce(ctx); err != nil {
			t.Logf("publish cycle: %v", err)
		}
		return h.countOutbox(ctx, outbox.StatusDeadLetter) > 0
	})

	// The ledger is untouched by all of that retrying.
	if got := h.balance(a); got != balanceBefore {
		t.Fatalf("INVARIANT 10 violated: balance changed from %d to %d during retries", balanceBefore, got)
	}
	if got := h.countEntries(ctx); got != entriesBefore {
		t.Fatalf("INVARIANT 10 violated: retries created %d entries", got-entriesBefore)
	}
	h.requireReconciled(ctx)
}

// TestOutboxRecoversAfterRestart simulates a worker dying between the Kafka
// publish and the bookkeeping update. The event is republished — at-least-once
// by design — and the row eventually completes.
func TestOutboxRecoversAfterRestart(t *testing.T) {
	h := newHarness(t)
	if !kafkaAvailable(h.cfg) {
		t.Skip("kafka not reachable")
	}
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")
	h.transfer(uuid.NewString(), a, b, 2_500, "USD")

	// This worker "crashes" after every publish, before marking the row.
	h.failpoints.Arm(failpoint.AfterKafkaPublish, "error")
	publisher := h.newPublisher()
	if _, err := publisher.RunOnce(ctx); err != nil {
		t.Fatalf("publish cycle: %v", err)
	}
	if h.countOutbox(ctx, outbox.StatusPublished) != 0 {
		t.Fatal("row was marked published even though the worker crashed first")
	}

	// A fresh worker starts up. Once the dead worker's claim TTL lapses, the
	// row becomes claimable again and the event is republished — at-least-once
	// delivery, deduplicated by consumers on event_id.
	h.failpoints.Reset()
	time.Sleep(h.cfg.OutboxClaimTTL + 100*time.Millisecond)
	restarted := h.newPublisher()
	eventually(t, 30*time.Second, "restarted worker to drain the outbox", func() bool {
		if _, err := restarted.RunOnce(ctx); err != nil {
			t.Logf("publish cycle: %v", err)
		}
		return h.countOutbox(ctx, outbox.StatusPublished) > 0
	})
	h.requireReconciled(ctx)
}

// TestOutboxClaimIsExclusive proves two workers never claim the same row, so
// replicas can scale out safely.
func TestOutboxClaimIsExclusive(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 100_000, "USD")
	for i := 0; i < 10; i++ {
		h.transfer(uuid.NewString(), a, b, 100, "USD")
	}

	first, err := outbox.Claim(ctx, h.db.Pool(), "worker-1", 100, h.cfg.OutboxClaimTTL)
	if err != nil {
		t.Fatalf("claim 1: %v", err)
	}
	second, err := outbox.Claim(ctx, h.db.Pool(), "worker-2", 100, h.cfg.OutboxClaimTTL)
	if err != nil {
		t.Fatalf("claim 2: %v", err)
	}

	if len(first) == 0 {
		t.Fatal("first worker claimed nothing")
	}
	if len(second) != 0 {
		t.Fatalf("second worker claimed %d rows already held by the first", len(second))
	}
}

// TestEventReplay republishes history and proves it changes nothing in the
// ledger and preserves the original event IDs (INVARIANT 10).
func TestEventReplay(t *testing.T) {
	h := newHarness(t)
	if !kafkaAvailable(h.cfg) {
		t.Skip("kafka not reachable")
	}
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	var txn ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 2_500, "USD").decode(t, &txn)

	original, err := outbox.ListForReplay(ctx, h.db.Pool(), txn.ID, nil, nil, 10)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(original) != 1 {
		t.Fatalf("want 1 event, got %d", len(original))
	}

	balanceBefore := h.balance(a)
	entriesBefore := h.countEntries(ctx)
	txBefore := h.countTransactions(ctx)

	result, err := h.replayer.Replay(ctx, outbox.ReplayRequest{
		TransactionID: txn.ID,
		RequestedBy:   "test",
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if result.Published != 1 {
		t.Fatalf("published %d events, want 1 (failed=%d)", result.Published, result.Failed)
	}
	// The replayed event carries the ORIGINAL id so consumers dedupe it.
	if result.EventIDs[0] != original[0].ID {
		t.Fatalf("replay changed the event id: %q -> %q", original[0].ID, result.EventIDs[0])
	}

	if got := h.balance(a); got != balanceBefore {
		t.Fatalf("INVARIANT 10 violated: replay changed the balance from %d to %d", balanceBefore, got)
	}
	if got := h.countEntries(ctx); got != entriesBefore {
		t.Fatalf("INVARIANT 10 violated: replay created %d entries", got-entriesBefore)
	}
	if got := h.countTransactions(ctx); got != txBefore {
		t.Fatalf("INVARIANT 10 violated: replay created %d transactions", got-txBefore)
	}
	h.requireReconciled(ctx)
}

// TestReplayIsDeterministicallyOrdered checks events come back in a stable
// (created_at, id) order.
func TestReplayIsDeterministicallyOrdered(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 100_000, "USD")
	for i := 0; i < 8; i++ {
		h.transfer(uuid.NewString(), a, b, 100, "USD")
	}

	from := time.Now().Add(-time.Hour)
	to := time.Now().Add(time.Hour)

	first, err := h.replayer.Replay(ctx, outbox.ReplayRequest{From: &from, To: &to, DryRun: true})
	if err != nil {
		t.Fatalf("replay 1: %v", err)
	}
	second, err := h.replayer.Replay(ctx, outbox.ReplayRequest{From: &from, To: &to, DryRun: true})
	if err != nil {
		t.Fatalf("replay 2: %v", err)
	}

	if len(first.EventIDs) != len(second.EventIDs) {
		t.Fatalf("replay matched %d then %d events", len(first.EventIDs), len(second.EventIDs))
	}
	for i := range first.EventIDs {
		if first.EventIDs[i] != second.EventIDs[i] {
			t.Fatalf("replay order is not deterministic at index %d: %q vs %q",
				i, first.EventIDs[i], second.EventIDs[i])
		}
	}
}

// TestReplayEndpointRequiresSelector rejects an unbounded replay.
func TestReplayEndpointRequiresSelector(t *testing.T) {
	h := newHarness(t)
	resp := h.post("/v1/replay", uuid.NewString(), map[string]any{})
	if resp.Status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body %s", resp.Status, resp.Body)
	}
	if code := resp.errorCode(t); code != "invalid_request" {
		t.Fatalf("error code = %q, want invalid_request", code)
	}
}

// TestReplayEndpointDryRun exercises the HTTP surface end to end.
func TestReplayEndpointDryRun(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	var txn ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 2_500, "USD").decode(t, &txn)

	resp := h.post("/v1/replay", uuid.NewString(), map[string]any{
		"transaction_id": txn.ID,
		"dry_run":        true,
	})
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", resp.Status, resp.Body)
	}
	var result outbox.ReplayResult
	resp.decode(t, &result)
	if result.Matched != 1 {
		t.Fatalf("matched %d events, want 1", result.Matched)
	}
	if result.Published != 0 {
		t.Fatalf("dry run published %d events, want 0", result.Published)
	}
	h.requireReconciled(ctx)
}
