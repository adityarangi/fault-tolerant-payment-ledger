// Package apperr defines the stable error codes returned by the HTTP API.
//
// Codes are part of the public contract: clients branch on them, so they must
// not change. HTTP status is derived from the code, never set independently.
package apperr

import (
	"errors"
	"fmt"
	"net/http"
)

// Code is a stable, machine-readable error identifier.
type Code string

const (
	CodeInvalidRequest             Code = "invalid_request"
	CodeUnknownAccount             Code = "unknown_account"
	CodeCurrencyMismatch           Code = "currency_mismatch"
	CodeInsufficientFunds          Code = "insufficient_funds"
	CodeIdempotencyConflict        Code = "idempotency_conflict"
	CodeIdempotencyInProgress      Code = "idempotency_in_progress"
	CodeTransactionAlreadyReversed Code = "transaction_already_reversed"
	CodeTransactionNotFound        Code = "transaction_not_found"
	CodeAccountExists              Code = "account_exists"
	CodeRateLimited                Code = "rate_limited"
	CodeDependencyUnavailable      Code = "dependency_unavailable"
	CodeInternal                   Code = "internal_error"
)

// Error is an application error carrying a stable code and a safe message.
type Error struct {
	Code    Code
	Message string
	// Details carries optional structured context for the client. It must
	// never contain secrets or raw driver errors.
	Details map[string]any
	// cause is logged but never serialised to the client.
	cause error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

// WithCause attaches an underlying error for logging.
func (e *Error) WithCause(err error) *Error {
	clone := *e
	clone.cause = err
	return &clone
}

// WithDetail attaches a structured detail field.
func (e *Error) WithDetail(key string, value any) *Error {
	clone := *e
	clone.Details = make(map[string]any, len(e.Details)+1)
	for k, v := range e.Details {
		clone.Details[k] = v
	}
	clone.Details[key] = value
	return &clone
}

// New builds an Error with the given code and message.
func New(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// HTTPStatus maps a code to its HTTP status.
func (e *Error) HTTPStatus() int {
	switch e.Code {
	case CodeInvalidRequest, CodeCurrencyMismatch:
		return http.StatusBadRequest
	case CodeUnknownAccount, CodeTransactionNotFound:
		return http.StatusNotFound
	case CodeInsufficientFunds:
		return http.StatusUnprocessableEntity
	case CodeIdempotencyConflict, CodeTransactionAlreadyReversed, CodeAccountExists:
		return http.StatusConflict
	case CodeIdempotencyInProgress:
		return http.StatusConflict
	case CodeRateLimited:
		return http.StatusTooManyRequests
	case CodeDependencyUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// From coerces any error into an *Error, defaulting to internal_error.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr
	}
	return (&Error{Code: CodeInternal, Message: "internal error"}).WithCause(err)
}

// Is reports whether err carries the given code.
func Is(err error, code Code) bool {
	var appErr *Error
	if errors.As(err, &appErr) {
		return appErr.Code == code
	}
	return false
}

// Common constructors used across packages.

func InvalidRequest(format string, args ...any) *Error {
	return New(CodeInvalidRequest, format, args...)
}

func UnknownAccount(id string) *Error {
	return New(CodeUnknownAccount, "account %q does not exist", id).WithDetail("account_id", id)
}

func CurrencyMismatch(format string, args ...any) *Error {
	return New(CodeCurrencyMismatch, format, args...)
}

func InsufficientFunds(accountID string) *Error {
	return New(CodeInsufficientFunds, "account %q has insufficient funds", accountID).
		WithDetail("account_id", accountID)
}

func DependencyUnavailable(dep string, cause error) *Error {
	return New(CodeDependencyUnavailable, "dependency %q is unavailable", dep).
		WithDetail("dependency", dep).WithCause(cause)
}

func Internal(cause error) *Error {
	return New(CodeInternal, "internal error").WithCause(cause)
}
