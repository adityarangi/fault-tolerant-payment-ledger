// Package observability provides structured logging, request/correlation IDs
// and Prometheus metrics shared by every binary in the system.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyCorrelationID
	ctxKeyTransactionID
	ctxKeyLogger
)

// NewLogger builds a JSON slog logger at the given level.
func NewLogger(service, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler).With(slog.String("service", service))
}

// WithRequestID returns a context carrying the request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// WithCorrelationID returns a context carrying the correlation ID, which is
// propagated across HTTP -> PostgreSQL -> Kafka -> webhook hops.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyCorrelationID, id)
}

// WithTransactionID returns a context carrying the ledger transaction ID.
func WithTransactionID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyTransactionID, id)
}

// RequestID extracts the request ID, or "" when absent.
func RequestID(ctx context.Context) string { return stringValue(ctx, ctxKeyRequestID) }

// CorrelationID extracts the correlation ID, or "" when absent.
func CorrelationID(ctx context.Context) string { return stringValue(ctx, ctxKeyCorrelationID) }

// TransactionID extracts the ledger transaction ID, or "" when absent.
func TransactionID(ctx context.Context) string { return stringValue(ctx, ctxKeyTransactionID) }

func stringValue(ctx context.Context, key ctxKey) string {
	if v, ok := ctx.Value(key).(string); ok {
		return v
	}
	return ""
}

// WithLogger stores a logger on the context.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKeyLogger, logger)
}

// Logger returns the context logger enriched with any IDs present on the
// context. It never returns nil.
func Logger(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(ctxKeyLogger).(*slog.Logger)
	if !ok || logger == nil {
		logger = slog.Default()
	}
	attrs := make([]any, 0, 6)
	if id := RequestID(ctx); id != "" {
		attrs = append(attrs, slog.String("request_id", id))
	}
	if id := CorrelationID(ctx); id != "" {
		attrs = append(attrs, slog.String("correlation_id", id))
	}
	if id := TransactionID(ctx); id != "" {
		attrs = append(attrs, slog.String("transaction_id", id))
	}
	if len(attrs) == 0 {
		return logger
	}
	return logger.With(attrs...)
}
