package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/adityarangi/payment-ledger/internal/observability"
)

type transferPayload struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      int64  `json:"amount"`
	Currency    string `json:"currency"`
}

// The request hash is what distinguishes "the same request retried" from "a
// different request reusing a key".
func TestHashRequestIsStableAndSensitive(t *testing.T) {
	payload := transferPayload{Source: "a", Destination: "b", Amount: 2500, Currency: "USD"}

	first, err := HashRequest("POST /v1/transfers", payload)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	second, err := HashRequest("POST /v1/transfers", payload)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if first != second {
		t.Fatal("hashing the same request twice produced different results")
	}

	changed := payload
	changed.Amount = 2501
	other, err := HashRequest("POST /v1/transfers", changed)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if other == first {
		t.Fatal("a different amount produced the same hash")
	}

	// The scope is part of the hash, so one key reused on two endpoints does
	// not look like the same request.
	scoped, err := HashRequest("POST /v1/replay", payload)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if scoped == first {
		t.Fatal("different scopes produced the same hash")
	}
}

func TestHashRequestMapOrderIndependence(t *testing.T) {
	a := map[string]any{"x": 1, "y": 2, "z": 3}
	b := map[string]any{"z": 3, "y": 2, "x": 1}

	hashA, err := HashRequest("s", a)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	hashB, err := HashRequest("s", b)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if hashA != hashB {
		t.Fatal("map key order changed the hash; retries would be seen as conflicts")
	}
}

func TestConflictAndInProgressErrors(t *testing.T) {
	conflict := Conflict("key-1")
	if conflict.Code != "idempotency_conflict" {
		t.Fatalf("code = %q", conflict.Code)
	}
	if conflict.HTTPStatus() != 409 {
		t.Fatalf("status = %d, want 409", conflict.HTTPStatus())
	}
	if conflict.Details["idempotency_key"] != "key-1" {
		t.Fatal("conflict error does not carry the key")
	}

	inProgress := InProgress("key-1")
	if inProgress.Code != "idempotency_in_progress" {
		t.Fatalf("code = %q", inProgress.Code)
	}
}

// A nil or disabled cache must behave like a permanent miss rather than
// panicking, because Redis is optional everywhere it is used.
func TestNilCacheIsSafe(t *testing.T) {
	metrics := observability.NewMetrics()
	cache := NewCache(nil, time.Minute, time.Second, metrics)

	if got := cache.Get(context.Background(), "scope", "key"); got != nil {
		t.Fatalf("disabled cache returned %+v, want nil", got)
	}
	// Must not panic.
	cache.Put(context.Background(), "scope", "key", CachedResponse{Status: 201})

	var nilCache *Cache
	if got := nilCache.Get(context.Background(), "scope", "key"); got != nil {
		t.Fatal("nil cache returned a value")
	}
	nilCache.Put(context.Background(), "scope", "key", CachedResponse{})
}
