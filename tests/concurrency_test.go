//go:build integration

package tests

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/adityarangi/payment-ledger/internal/ledger"
)

// TestConcurrentDuplicateRequests fires the same idempotency key from many
// goroutines at once. Exactly one ledger transaction may result, and every
// caller must see the same response body (INVARIANT 4).
func TestConcurrentDuplicateRequests(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 100_000, "USD")

	const concurrency = 16
	key := uuid.NewString()

	before := h.countTransactions(ctx)

	var wg sync.WaitGroup
	results := make([]apiResponse, concurrency)
	start := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release all goroutines at once to maximise contention
			results[i] = h.transfer(key, a, b, 2_500, "USD")
		}(i)
	}
	close(start)
	wg.Wait()

	created := 0
	var body string
	for i, resp := range results {
		switch resp.Status {
		case http.StatusCreated:
			created++
			if body == "" {
				body = string(resp.Body)
			} else if string(resp.Body) != body {
				t.Fatalf("response %d differs from the first success", i)
			}
		case http.StatusConflict:
			// idempotency_in_progress is an acceptable outcome for a caller
			// that raced the original request.
			if code := resp.errorCode(t); code != "idempotency_in_progress" {
				t.Fatalf("response %d: unexpected conflict %q: %s", i, code, resp.Body)
			}
		default:
			t.Fatalf("response %d: unexpected status %d: %s", i, resp.Status, resp.Body)
		}
	}
	if created == 0 {
		t.Fatal("no request succeeded")
	}

	if got := h.countTransactions(ctx) - before; got != 1 {
		t.Fatalf("INVARIANT 4 violated: %d ledger transactions created, want exactly 1", got)
	}
	if got := h.balance(a); got != 97_500 {
		t.Fatalf("source balance = %d, want 97500 (money moved exactly once)", got)
	}
	h.requireReconciled(ctx)
}

// TestConcurrentOverspending launches many transfers that together exceed the
// balance. The account must never go negative (INVARIANT 7) and the successes
// must exactly account for the money that left.
func TestConcurrentOverspending(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")

	const (
		funded      = 10_000
		transferAmt = 1_000
		attempts    = 25 // 25 x 1000 = 25000, far more than the 10000 available
	)
	h.fund(system, a, funded, "USD")

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		succeeded int
		rejected  int
	)
	start := make(chan struct{})

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp := h.transfer(uuid.NewString(), a, b, transferAmt, "USD")

			mu.Lock()
			defer mu.Unlock()
			switch resp.Status {
			case http.StatusCreated:
				succeeded++
			case http.StatusUnprocessableEntity:
				rejected++
			default:
				t.Errorf("unexpected status %d: %s", resp.Status, resp.Body)
			}
		}()
	}
	close(start)
	wg.Wait()

	if succeeded != funded/transferAmt {
		t.Fatalf("%d transfers succeeded, want exactly %d", succeeded, funded/transferAmt)
	}
	if rejected != attempts-succeeded {
		t.Fatalf("%d rejected, want %d", rejected, attempts-succeeded)
	}
	if got := h.balance(a); got != 0 {
		t.Fatalf("INVARIANT 7 violated: balance = %d, want 0 and never negative", got)
	}
	if got := h.balance(b); got != funded {
		t.Fatalf("destination balance = %d, want %d", got, funded)
	}
	h.requireReconciled(ctx)
}

// TestOpposingTransfersDoNotDeadlock runs transfers in both directions
// simultaneously. Deterministic lock ordering means they queue rather than
// deadlock (INVARIANT 8); the test fails loudly if it stalls.
func TestOpposingTransfersDoNotDeadlock(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 100_000, "USD")
	h.fund(system, b, 100_000, "USD")

	const rounds = 40
	var wg sync.WaitGroup
	errs := make(chan string, rounds*2)
	start := make(chan struct{})

	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			if resp := h.transfer(uuid.NewString(), a, b, 100, "USD"); resp.Status != http.StatusCreated {
				errs <- string(resp.Body)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			if resp := h.transfer(uuid.NewString(), b, a, 100, "USD"); resp.Status != http.StatusCreated {
				errs <- string(resp.Body)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	close(start)
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("INVARIANT 8 violated: opposing transfers did not complete; likely deadlocked")
	}
	close(errs)
	for msg := range errs {
		t.Fatalf("transfer failed: %s", msg)
	}

	// Every A->B is matched by a B->A, so both balances return to where they
	// started.
	if got := h.balance(a); got != 100_000 {
		t.Fatalf("balance a = %d, want 100000", got)
	}
	if got := h.balance(b); got != 100_000 {
		t.Fatalf("balance b = %d, want 100000", got)
	}
	h.requireReconciled(ctx)
}

// TestConcurrentReversalsCreateOne proves the partial unique index holds under
// concurrency: many simultaneous reversal requests produce exactly one.
func TestConcurrentReversalsCreateOne(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	system, a, b := h.newAccounts("USD")
	h.fund(system, a, 100_000, "USD")

	var original ledger.Transaction
	h.transfer(uuid.NewString(), a, b, 5_000, "USD").decode(t, &original)

	const concurrency = 8
	path := "/v1/transactions/" + original.ID + "/reverse"

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
	)
	start := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Distinct keys: each is a genuine reversal attempt.
			resp := h.post(path, uuid.NewString(), map[string]any{"reason": "race"})
			mu.Lock()
			defer mu.Unlock()
			if resp.Status == http.StatusCreated {
				created++
			}
		}()
	}
	close(start)
	wg.Wait()

	if created != 1 {
		t.Fatalf("%d reversals were created, want exactly 1", created)
	}
	if got := h.balance(a); got != 100_000 {
		t.Fatalf("balance = %d, want 100000 after exactly one reversal", got)
	}
	h.requireReconciled(ctx)
}
