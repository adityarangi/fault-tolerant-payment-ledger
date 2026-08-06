//go:build integration

package tests

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/ledger"
)

// TestFailureBeforeCommit proves that a crash anywhere inside the payment
// transaction leaves no partial state at all (INVARIANT 5), and that the
// idempotency key remains usable afterwards.
func TestFailureBeforeCommit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	txBefore := h.countTransactions(ctx)
	entriesBefore := h.countEntries(ctx)
	outboxBefore := h.countOutbox(ctx, "")

	key := uuid.NewString()
	body := map[string]any{
		"source_account_id":      a,
		"destination_account_id": b,
		"amount":                 2_500,
		"currency":               "USD",
	}

	// Every failpoint inside the transaction must roll everything back.
	for _, fpName := range []string{
		failpoint.AfterAccountsLocked,
		failpoint.AfterEntriesWritten,
		failpoint.AfterBalancesUpdated,
		failpoint.AfterOutboxWritten,
		failpoint.BeforeCommit,
	} {
		resp := h.postWithFailpoint("/v1/transfers", key, fpName+"=error", body)
		if resp.Status != http.StatusInternalServerError {
			t.Fatalf("%s: status = %d, want 500; body %s", fpName, resp.Status, resp.Body)
		}

		if got := h.countTransactions(ctx); got != txBefore {
			t.Fatalf("%s: INVARIANT 5 violated: %d transactions leaked", fpName, got-txBefore)
		}
		if got := h.countEntries(ctx); got != entriesBefore {
			t.Fatalf("%s: INVARIANT 5 violated: %d entries leaked", fpName, got-entriesBefore)
		}
		if got := h.countOutbox(ctx, ""); got != outboxBefore {
			t.Fatalf("%s: INVARIANT 5 violated: %d outbox rows leaked", fpName, got-outboxBefore)
		}
		if got := h.balance(a); got != 10_000 {
			t.Fatalf("%s: balance = %d, want unchanged 10000", fpName, got)
		}
	}

	// The idempotency record was rolled back too, so the same key still works.
	resp := h.post("/v1/transfers", key, body)
	if resp.Status != http.StatusCreated {
		t.Fatalf("retry after failure: status %d body %s", resp.Status, resp.Body)
	}
	if got := h.balance(a); got != 7_500 {
		t.Fatalf("balance = %d, want 7500", got)
	}
	h.requireReconciled(ctx)
}

// TestFailureAfterCommitBeforeResponse simulates the classic lost response:
// the money moved, but the client never learned the outcome. Retrying with the
// same key must return the original result and must not move money again
// (INVARIANT 6).
func TestFailureAfterCommitBeforeResponse(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	key := uuid.NewString()
	body := map[string]any{
		"source_account_id":      a,
		"destination_account_id": b,
		"amount":                 2_500,
		"currency":               "USD",
	}

	// The commit succeeds; the response is lost on the way out.
	lost := h.postWithFailpoint("/v1/transfers", key, failpoint.AfterCommit+"=error", body)
	if lost.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", lost.Status, lost.Body)
	}

	// The payment really did happen.
	if got := h.balance(a); got != 7_500 {
		t.Fatalf("balance = %d, want 7500; the transaction committed", got)
	}
	countAfterCommit := h.countTransactions(ctx)

	// The client retries, unaware. It gets the original response back.
	retry := h.post("/v1/transfers", key, body)
	if retry.Status != http.StatusCreated {
		t.Fatalf("retry: status = %d, want 201; body %s", retry.Status, retry.Body)
	}
	if retry.Headers.Get("X-Idempotent-Replay") != "true" {
		t.Fatal("retry was not marked as an idempotent replay")
	}
	if got := h.countTransactions(ctx); got != countAfterCommit {
		t.Fatalf("INVARIANT 6 violated: retry created %d extra transactions", got-countAfterCommit)
	}
	if got := h.balance(a); got != 7_500 {
		t.Fatalf("INVARIANT 6 violated: balance = %d, money moved twice", got)
	}

	// The replayed body describes the transaction that actually committed.
	var txn ledger.Transaction
	retry.decode(t, &txn)
	if txn.ID == "" {
		t.Fatal("replayed response has no transaction id")
	}
	h.requireReconciled(ctx)
}

// TestFailureBeforeTransactionBegin proves a failure before BEGIN leaves no
// idempotency record, so the key is not burned.
func TestFailureBeforeTransactionBegin(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	key := uuid.NewString()
	body := map[string]any{
		"source_account_id":      a,
		"destination_account_id": b,
		"amount":                 1_000,
		"currency":               "USD",
	}

	resp := h.postWithFailpoint("/v1/transfers", key, failpoint.BeforeTxBegin+"=error", body)
	if resp.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", resp.Status, resp.Body)
	}

	var recorded int
	if err := h.db.Pool().QueryRow(ctx,
		`SELECT count(*) FROM idempotency_records WHERE key = $1`, key).Scan(&recorded); err != nil {
		t.Fatalf("count idempotency records: %v", err)
	}
	if recorded != 0 {
		t.Fatalf("%d idempotency records were left behind by a pre-BEGIN failure", recorded)
	}

	if retry := h.post("/v1/transfers", key, body); retry.Status != http.StatusCreated {
		t.Fatalf("retry: status %d body %s", retry.Status, retry.Body)
	}
	h.requireReconciled(ctx)
}

// TestPanicDuringTransactionRollsBack proves even a panic mid-transaction
// cannot leave partial state or wedge a connection.
func TestPanicDuringTransactionRollsBack(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	before := h.countEntries(ctx)
	body := map[string]any{
		"source_account_id":      a,
		"destination_account_id": b,
		"amount":                 2_500,
		"currency":               "USD",
	}

	resp := h.postWithFailpoint("/v1/transfers", uuid.NewString(), failpoint.BeforeCommit+"=panic", body)
	if resp.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body %s", resp.Status, resp.Body)
	}
	if got := h.countEntries(ctx); got != before {
		t.Fatalf("panic leaked %d entries", got-before)
	}
	if got := h.balance(a); got != 10_000 {
		t.Fatalf("balance = %d, want unchanged 10000", got)
	}

	// The pool is still usable afterwards.
	if resp := h.transfer(uuid.NewString(), a, b, 100, "USD"); resp.Status != http.StatusCreated {
		t.Fatalf("subsequent transfer failed: status %d body %s", resp.Status, resp.Body)
	}
	h.requireReconciled(ctx)
}

// TestRedisUnavailable points the cache and rate limiter at a dead Redis. The
// ledger must remain fully correct (INVARIANT 12).
func TestRedisUnavailable(t *testing.T) {
	h := newHarness(t, func(cfg *config.Config) {
		// A port nothing is listening on.
		cfg.RedisAddr = "127.0.0.1:6399"
		cfg.RateLimitEnabled = true
	})
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	key := uuid.NewString()
	first := h.transfer(key, a, b, 2_500, "USD")
	if first.Status != http.StatusCreated {
		t.Fatalf("transfer with Redis down: status %d body %s", first.Status, first.Body)
	}

	// Idempotency still works, served from PostgreSQL.
	before := h.countTransactions(ctx)
	second := h.transfer(key, a, b, 2_500, "USD")
	if second.Status != http.StatusCreated {
		t.Fatalf("retry with Redis down: status %d body %s", second.Status, second.Body)
	}
	if string(second.Body) != string(first.Body) {
		t.Fatal("replay body differs with Redis down")
	}
	if got := h.countTransactions(ctx); got != before {
		t.Fatalf("retry created %d extra transactions with Redis down", got-before)
	}

	// Conflict detection still works without the cache.
	conflict := h.transfer(key, a, b, 9_999, "USD")
	if conflict.Status != http.StatusConflict {
		t.Fatalf("conflict detection failed with Redis down: status %d", conflict.Status)
	}
	if got := h.balance(a); got != 7_500 {
		t.Fatalf("balance = %d, want 7500", got)
	}
	h.requireReconciled(ctx)
}

// TestRedisFlushedMidFlight flushes the cache between the original request and
// its retry, proving the cache is never load-bearing.
func TestRedisFlushedMidFlight(t *testing.T) {
	h := newHarness(t)
	if h.redis == nil {
		t.Skip("redis not configured")
	}
	ctx := context.Background()
	if err := h.redis.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}

	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	key := uuid.NewString()
	first := h.transfer(key, a, b, 2_500, "USD")
	if first.Status != http.StatusCreated {
		t.Fatalf("transfer: status %d body %s", first.Status, first.Body)
	}

	if err := h.redis.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	before := h.countTransactions(ctx)
	second := h.transfer(key, a, b, 2_500, "USD")
	if string(second.Body) != string(first.Body) {
		t.Fatalf("after FLUSHDB the replay differs:\nfirst:  %s\nsecond: %s", first.Body, second.Body)
	}
	if got := h.countTransactions(ctx); got != before {
		t.Fatalf("INVARIANT 12 violated: flushing Redis allowed %d duplicate transactions", got-before)
	}
	h.requireReconciled(ctx)
}
