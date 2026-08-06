//go:build integration

package tests

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/adityarangi/payment-ledger/internal/ledger"
)

// TestBalancedTransfer is the happy path: money moves, the transaction has two
// entries that sum to zero, and both balances reflect it.
func TestBalancedTransfer(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	resp := h.transfer(uuid.NewString(), a, b, 2_500, "USD")
	if resp.Status != http.StatusCreated {
		t.Fatalf("transfer: status %d body %s", resp.Status, resp.Body)
	}

	var txn ledger.Transaction
	resp.decode(t, &txn)

	if len(txn.Entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(txn.Entries))
	}
	if sum := txn.Sum(); sum != 0 {
		t.Fatalf("INVARIANT 1 violated: entries sum to %d, want 0", sum)
	}
	if got := h.balance(a); got != 7_500 {
		t.Fatalf("source balance = %d, want 7500", got)
	}
	if got := h.balance(b); got != 2_500 {
		t.Fatalf("destination balance = %d, want 2500", got)
	}
	h.requireReconciled(ctx)
}

// TestInsufficientFunds proves a non-overdraft account cannot be overdrawn and
// that the rejected attempt leaves nothing behind.
func TestInsufficientFunds(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 1_000, "USD")

	before := h.countEntries(ctx)
	resp := h.transfer(uuid.NewString(), a, b, 5_000, "USD")

	if resp.Status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body %s", resp.Status, resp.Body)
	}
	if code := resp.errorCode(t); code != "insufficient_funds" {
		t.Fatalf("error code = %q, want insufficient_funds", code)
	}
	if got := h.balance(a); got != 1_000 {
		t.Fatalf("balance changed to %d after a rejected transfer", got)
	}
	if after := h.countEntries(ctx); after != before {
		t.Fatalf("rejected transfer wrote %d entries", after-before)
	}
	h.requireReconciled(ctx)
}

// TestCurrencyMismatch rejects a transfer whose currency disagrees with the
// accounts.
func TestCurrencyMismatch(t *testing.T) {
	h := newHarness(t)
	system, a, _ := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	eurAccount := "eur-" + uuid.NewString()[:8]
	h.createAccount(eurAccount, "EUR", ledger.KindUser, false)

	resp := h.transfer(uuid.NewString(), a, eurAccount, 1_000, "USD")
	if resp.Status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body %s", resp.Status, resp.Body)
	}
	if code := resp.errorCode(t); code != "currency_mismatch" {
		t.Fatalf("error code = %q, want currency_mismatch", code)
	}
	h.requireReconciled(context.Background())
}

// TestInvalidAmount rejects zero and negative amounts. Money is a signed
// integer, so "negative transfer" must be an explicit error rather than a
// sneaky reverse transfer.
func TestInvalidAmount(t *testing.T) {
	h := newHarness(t)
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	for _, amount := range []int64{0, -1, -5_000} {
		resp := h.transfer(uuid.NewString(), a, b, amount, "USD")
		if resp.Status != http.StatusBadRequest {
			t.Fatalf("amount %d: status = %d, want 400; body %s", amount, resp.Status, resp.Body)
		}
		if code := resp.errorCode(t); code != "invalid_request" {
			t.Fatalf("amount %d: error code = %q, want invalid_request", amount, code)
		}
	}
	if got := h.balance(a); got != 10_000 {
		t.Fatalf("balance = %d, want unchanged 10000", got)
	}
}

// TestUnknownAccount rejects transfers naming an account that does not exist.
func TestUnknownAccount(t *testing.T) {
	h := newHarness(t)
	system, a, _ := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	resp := h.transfer(uuid.NewString(), a, "does-not-exist", 100, "USD")
	if resp.Status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body %s", resp.Status, resp.Body)
	}
	if code := resp.errorCode(t); code != "unknown_account" {
		t.Fatalf("error code = %q, want unknown_account", code)
	}
}

// TestIdempotencyMissingKey enforces the header on every mutation.
func TestIdempotencyMissingKey(t *testing.T) {
	h := newHarness(t)
	resp := h.do(http.MethodPost, "/v1/transfers", map[string]any{
		"source_account_id": "a", "destination_account_id": "b",
		"amount": 100, "currency": "USD",
	}, nil)
	if resp.Status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body %s", resp.Status, resp.Body)
	}
	if code := resp.errorCode(t); code != "invalid_request" {
		t.Fatalf("error code = %q, want invalid_request", code)
	}
}

// TestDuplicateIdempotentRequest replays the original response and proves only
// one ledger transaction was created (INVARIANT 4).
func TestDuplicateIdempotentRequest(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	key := uuid.NewString()
	first := h.transfer(key, a, b, 2_500, "USD")
	if first.Status != http.StatusCreated {
		t.Fatalf("first transfer: status %d body %s", first.Status, first.Body)
	}
	countAfterFirst := h.countTransactions(ctx)

	second := h.transfer(key, a, b, 2_500, "USD")
	if second.Status != first.Status {
		t.Fatalf("replay status = %d, want %d", second.Status, first.Status)
	}
	if string(second.Body) != string(first.Body) {
		t.Fatalf("replay body differs:\nfirst:  %s\nsecond: %s", first.Body, second.Body)
	}
	if got := h.countTransactions(ctx); got != countAfterFirst {
		t.Fatalf("INVARIANT 4 violated: retry created %d extra transactions", got-countAfterFirst)
	}
	if got := h.balance(a); got != 7_500 {
		t.Fatalf("balance = %d, want 7500 (retry must not move money twice)", got)
	}
	h.requireReconciled(ctx)
}

// TestConflictingIdempotencyPayload rejects key reuse with a different body.
func TestConflictingIdempotencyPayload(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	key := uuid.NewString()
	if resp := h.transfer(key, a, b, 2_500, "USD"); resp.Status != http.StatusCreated {
		t.Fatalf("first transfer: status %d body %s", resp.Status, resp.Body)
	}

	conflict := h.transfer(key, a, b, 9_999, "USD")
	if conflict.Status != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", conflict.Status, conflict.Body)
	}
	if code := conflict.errorCode(t); code != "idempotency_conflict" {
		t.Fatalf("error code = %q, want idempotency_conflict", code)
	}
	if got := h.balance(a); got != 7_500 {
		t.Fatalf("balance = %d, want 7500; conflicting retry must not move money", got)
	}
	h.requireReconciled(ctx)
}

// TestReversal restores the financial effect while preserving history
// (INVARIANT 9).
func TestReversal(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	transferResp := h.transfer(uuid.NewString(), a, b, 2_500, "USD")
	var original ledger.Transaction
	transferResp.decode(t, &original)

	reverseResp := h.post("/v1/transactions/"+original.ID+"/reverse", uuid.NewString(),
		map[string]any{"reason": "customer dispute"})
	if reverseResp.Status != http.StatusCreated {
		t.Fatalf("reverse: status %d body %s", reverseResp.Status, reverseResp.Body)
	}

	var reversal ledger.Transaction
	reverseResp.decode(t, &reversal)
	if reversal.Kind != ledger.TxKindReversal {
		t.Fatalf("reversal kind = %q, want reversal", reversal.Kind)
	}
	if reversal.ReversesTransactionID == nil || *reversal.ReversesTransactionID != original.ID {
		t.Fatalf("reversal does not point at the original transaction")
	}
	if sum := reversal.Sum(); sum != 0 {
		t.Fatalf("reversal entries sum to %d, want 0", sum)
	}

	// Balances are restored...
	if got := h.balance(a); got != 10_000 {
		t.Fatalf("source balance = %d, want 10000 after reversal", got)
	}
	if got := h.balance(b); got != 0 {
		t.Fatalf("destination balance = %d, want 0 after reversal", got)
	}

	// ...while the original transaction and its entries remain untouched.
	fetched := h.get("/v1/transactions/" + original.ID)
	var stored ledger.Transaction
	fetched.decode(t, &stored)
	if len(stored.Entries) != 2 {
		t.Fatalf("original transaction lost entries: %d", len(stored.Entries))
	}
	if stored.ReversedByTransactionID == nil || *stored.ReversedByTransactionID != reversal.ID {
		t.Fatalf("original transaction does not link to its reversal")
	}
	h.requireReconciled(ctx)
}

// TestDuplicateReversal proves a transaction can be reversed only once, even
// with a fresh idempotency key.
func TestDuplicateReversal(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	var original ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 2_500, "USD").decode(t, &original)

	path := "/v1/transactions/" + original.ID + "/reverse"
	if resp := h.post(path, uuid.NewString(), map[string]any{"reason": "first"}); resp.Status != http.StatusCreated {
		t.Fatalf("first reversal: status %d body %s", resp.Status, resp.Body)
	}

	// A different key means this is genuinely a second reversal request, not
	// an idempotent retry.
	second := h.post(path, uuid.NewString(), map[string]any{"reason": "second"})
	if second.Status != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body %s", second.Status, second.Body)
	}
	if code := second.errorCode(t); code != "transaction_already_reversed" {
		t.Fatalf("error code = %q, want transaction_already_reversed", code)
	}
	if got := h.balance(a); got != 10_000 {
		t.Fatalf("balance = %d, want 10000; a rejected second reversal must not move money", got)
	}
	h.requireReconciled(ctx)
}

// TestReversalIsIdempotent replays the same reversal response for a repeated
// key rather than reversing twice.
func TestReversalIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	var original ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 2_500, "USD").decode(t, &original)

	path := "/v1/transactions/" + original.ID + "/reverse"
	key := uuid.NewString()
	first := h.post(path, key, map[string]any{"reason": "dispute"})
	before := h.countTransactions(ctx)

	second := h.post(path, key, map[string]any{"reason": "dispute"})
	if string(second.Body) != string(first.Body) {
		t.Fatalf("replayed reversal differs:\nfirst:  %s\nsecond: %s", first.Body, second.Body)
	}
	if got := h.countTransactions(ctx); got != before {
		t.Fatalf("replayed reversal created %d extra transactions", got-before)
	}
	h.requireReconciled(ctx)
}

// TestLedgerEntriesAreImmutable checks INVARIANT 2 at the database level: even
// a direct UPDATE or DELETE is refused.
func TestLedgerEntriesAreImmutable(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 10_000, "USD")

	var txn ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 2_500, "USD").decode(t, &txn)

	if _, err := h.db.Pool().Exec(ctx,
		`UPDATE ledger_entries SET amount = amount + 1 WHERE transaction_id = $1`, txn.ID); err == nil {
		t.Fatal("INVARIANT 2 violated: ledger entries were updatable")
	}
	if _, err := h.db.Pool().Exec(ctx,
		`DELETE FROM ledger_entries WHERE transaction_id = $1`, txn.ID); err == nil {
		t.Fatal("INVARIANT 2 violated: ledger entries were deletable")
	}
	if _, err := h.db.Pool().Exec(ctx,
		`UPDATE ledger_transactions SET description = 'tampered' WHERE id = $1`, txn.ID); err == nil {
		t.Fatal("INVARIANT 2 violated: ledger transactions were updatable")
	}
	h.requireReconciled(ctx)
}

// TestUnbalancedTransactionRejected proves the zero-sum rule is enforced by
// PostgreSQL, not just by Go: a hand-written unbalanced pair fails at COMMIT.
func TestUnbalancedTransactionRejected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, a, b := h.newAccounts("USD")

	tx, err := h.db.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	txID := uuid.NewString()
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_transactions (id, kind, currency) VALUES ($1, 'transfer', 'USD')`, txID); err != nil {
		t.Fatalf("insert transaction: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_entries (id, transaction_id, account_id, amount, currency, seq)
         VALUES ($1, $2, $3, -100, 'USD', 0)`, uuid.NewString(), txID, a); err != nil {
		t.Fatalf("insert debit: %v", err)
	}
	// Deliberately mismatched credit.
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_entries (id, transaction_id, account_id, amount, currency, seq)
         VALUES ($1, $2, $3, 250, 'USD', 1)`, uuid.NewString(), txID, b); err != nil {
		t.Fatalf("insert credit: %v", err)
	}

	if err := tx.Commit(ctx); err == nil {
		t.Fatal("INVARIANT 1 violated: an unbalanced transaction committed")
	}
}

// TestSingleEntryTransactionRejected proves a transaction cannot have fewer
// than two entries.
func TestSingleEntryTransactionRejected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, a, _ := h.newAccounts("USD")

	tx, err := h.db.Pool().Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	txID := uuid.NewString()
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_transactions (id, kind, currency) VALUES ($1, 'transfer', 'USD')`, txID); err != nil {
		t.Fatalf("insert transaction: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_entries (id, transaction_id, account_id, amount, currency, seq)
         VALUES ($1, $2, $3, 0, 'USD', 0)`, uuid.NewString(), txID, a); err == nil {
		// A zero-amount entry is rejected outright by a CHECK constraint.
		t.Fatal("zero-amount entry was accepted")
	}
}

// TestBalanceReconciliation recomputes balances from entries after a mix of
// activity (INVARIANT 3).
func TestBalanceReconciliation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 50_000, "USD")
	h.fund(system, b, 20_000, "USD")

	for i := 0; i < 10; i++ {
		h.transfer(uuid.NewString(), a, b, 1_000, "USD")
		h.transfer(uuid.NewString(), b, a, 400, "USD")
	}
	var toReverse ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 3_000, "USD").decode(t, &toReverse)
	h.post("/v1/transactions/"+toReverse.ID+"/reverse", uuid.NewString(), map[string]any{"reason": "test"})

	report, err := h.reconciler.Run(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !report.OK() {
		t.Fatalf("reconciliation failed: %+v", report)
	}

	// The system account's negative balance is exactly the money it issued.
	if got := h.balance(system); got != -70_000 {
		t.Fatalf("system balance = %d, want -70000", got)
	}
	if got := h.balance(a) + h.balance(b) + h.balance(system); got != 0 {
		t.Fatalf("all balances must sum to zero in a closed double-entry system, got %d", got)
	}

	// The endpoint reports the same thing.
	resp := h.get("/v1/reconciliation")
	if resp.Status != http.StatusOK {
		t.Fatalf("reconciliation endpoint: status %d body %s", resp.Status, resp.Body)
	}
}

// TestAccountEntryHistory checks the entries endpoint and its pagination.
func TestAccountEntryHistory(t *testing.T) {
	h := newHarness(t)
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 50_000, "USD")
	for i := 0; i < 5; i++ {
		h.transfer(uuid.NewString(), a, b, 100, "USD")
	}

	resp := h.get("/v1/accounts/" + a + "/entries?limit=3")
	if resp.Status != http.StatusOK {
		t.Fatalf("entries: status %d body %s", resp.Status, resp.Body)
	}
	var page ledger.EntryPage
	resp.decode(t, &page)
	if len(page.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(page.Entries))
	}
	if page.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}

	next := h.get("/v1/accounts/" + a + "/entries?limit=10&cursor=" + page.NextCursor)
	var page2 ledger.EntryPage
	next.decode(t, &page2)
	// 1 funding + 5 transfers = 6 entries total, 3 already seen.
	if len(page2.Entries) != 3 {
		t.Fatalf("second page has %d entries, want 3", len(page2.Entries))
	}
	for _, e := range page2.Entries {
		for _, seen := range page.Entries {
			if e.ID == seen.ID {
				t.Fatalf("entry %s appeared on both pages", e.ID)
			}
		}
	}
}
