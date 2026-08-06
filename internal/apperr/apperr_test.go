package apperr

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

// The HTTP status is derived from the code, so clients can rely on both.
func TestHTTPStatusMapping(t *testing.T) {
	cases := map[Code]int{
		CodeInvalidRequest:             http.StatusBadRequest,
		CodeCurrencyMismatch:           http.StatusBadRequest,
		CodeUnknownAccount:             http.StatusNotFound,
		CodeTransactionNotFound:        http.StatusNotFound,
		CodeInsufficientFunds:          http.StatusUnprocessableEntity,
		CodeIdempotencyConflict:        http.StatusConflict,
		CodeTransactionAlreadyReversed: http.StatusConflict,
		CodeAccountExists:              http.StatusConflict,
		CodeRateLimited:                http.StatusTooManyRequests,
		CodeDependencyUnavailable:      http.StatusServiceUnavailable,
		CodeInternal:                   http.StatusInternalServerError,
	}
	for code, want := range cases {
		got := (&Error{Code: code}).HTTPStatus()
		if got != want {
			t.Errorf("code %s: status = %d, want %d", code, got, want)
		}
	}
}

// An unrecognised error must become internal_error rather than leaking driver
// detail to the client.
func TestFromWrapsUnknownErrors(t *testing.T) {
	raw := errors.New("pq: connection reset by peer")
	appErr := From(raw)

	if appErr.Code != CodeInternal {
		t.Fatalf("code = %s, want internal_error", appErr.Code)
	}
	if appErr.Message == raw.Error() {
		t.Fatal("the raw driver error leaked into the client-facing message")
	}
	if !errors.Is(appErr, raw) {
		t.Fatal("the original error is not retrievable for logging")
	}
	if From(nil) != nil {
		t.Fatal("From(nil) should be nil")
	}
}

func TestFromPreservesApplicationErrors(t *testing.T) {
	original := InsufficientFunds("acct-1")
	wrapped := fmt.Errorf("ledger: %w", original)

	got := From(wrapped)
	if got.Code != CodeInsufficientFunds {
		t.Fatalf("code = %s, want insufficient_funds", got.Code)
	}
	if got.Details["account_id"] != "acct-1" {
		t.Fatalf("details lost: %v", got.Details)
	}
}

func TestIs(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", UnknownAccount("acct-9"))
	if !Is(err, CodeUnknownAccount) {
		t.Fatal("Is did not match through wrapping")
	}
	if Is(err, CodeInsufficientFunds) {
		t.Fatal("Is matched the wrong code")
	}
	if Is(nil, CodeInternal) {
		t.Fatal("Is(nil) matched")
	}
}

// WithDetail and WithCause must not mutate the receiver, since the package
// exposes shared constructors.
func TestWithDetailDoesNotMutate(t *testing.T) {
	base := New(CodeInvalidRequest, "bad")
	withOne := base.WithDetail("field", "amount")
	withTwo := withOne.WithDetail("reason", "negative")

	if len(base.Details) != 0 {
		t.Fatalf("base error was mutated: %v", base.Details)
	}
	if len(withOne.Details) != 1 {
		t.Fatalf("withOne has %d details, want 1", len(withOne.Details))
	}
	if len(withTwo.Details) != 2 {
		t.Fatalf("withTwo has %d details, want 2", len(withTwo.Details))
	}

	cause := errors.New("root cause")
	withCause := base.WithCause(cause)
	if !errors.Is(withCause, cause) {
		t.Fatal("cause not attached")
	}
	if errors.Unwrap(base) != nil {
		t.Fatal("base error gained a cause")
	}
}
