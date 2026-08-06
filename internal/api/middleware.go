package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/adityarangi/payment-ledger/internal/apperr"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/observability"
)

// Header names used across the API.
const (
	HeaderRequestID      = "X-Request-Id"
	HeaderCorrelationID  = "X-Correlation-Id"
	HeaderIdempotencyKey = "Idempotency-Key"
	HeaderFailpoint      = "X-Failpoint"
	HeaderIdempotentHit  = "X-Idempotent-Replay"
)

type statusRecorder struct {
	http.ResponseWriter
	status int
	// code carries the application error code for the metrics label.
	code    string
	written bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.written {
		return
	}
	r.status = status
	r.written = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

// withRequestContext assigns request and correlation IDs and attaches a
// request-scoped logger. The correlation ID is honoured from the client when
// supplied so a payment can be traced across the API, Kafka and webhooks.
func (s *Server) withRequestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(HeaderRequestID)
		if requestID == "" {
			requestID = uuid.NewString()
		}
		correlationID := r.Header.Get(HeaderCorrelationID)
		if correlationID == "" {
			correlationID = requestID
		}

		ctx := observability.WithLogger(r.Context(), s.logger)
		ctx = observability.WithRequestID(ctx, requestID)
		ctx = observability.WithCorrelationID(ctx, correlationID)

		w.Header().Set(HeaderRequestID, requestID)
		w.Header().Set(HeaderCorrelationID, correlationID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withRecovery converts a panic into a 500 without killing the process. A
// panicking failpoint must not take the server down mid-test.
func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				observability.Logger(r.Context()).Error("panic serving request",
					"panic", rec, "path", r.URL.Path)
				s.writeError(w, r, apperr.New(apperr.CodeInternal, "internal error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withObservability records structured access logs and Prometheus metrics.
func (s *Server) withObservability(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		duration := time.Since(start)
		status := strconv.Itoa(rec.status)
		s.metrics.HTTPRequests.WithLabelValues(r.Method, route, status, rec.code).Inc()
		s.metrics.HTTPDuration.WithLabelValues(r.Method, route, status).Observe(duration.Seconds())

		observability.Logger(r.Context()).Info("http request",
			"method", r.Method,
			"route", route,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", duration.Milliseconds(),
			"error_code", rec.code,
		)
	})
}

// withFailpoints applies per-request failpoint overrides from the X-Failpoint
// header. Ignored entirely unless failpoints are enabled for this process.
func (s *Server) withFailpoints(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spec := r.Header.Get(HeaderFailpoint)
		if spec == "" || !s.failpoints.Enabled() {
			next.ServeHTTP(w, r)
			return
		}
		ctx, err := failpoint.WithRequestFailpoints(r.Context(), spec)
		if err != nil {
			s.writeError(w, r, apperr.InvalidRequest("%s", err.Error()))
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withRateLimit rejects requests over the configured budget.
//
// The limiter lives in Redis, so it is shared across API replicas. It is an
// availability control, not a correctness one: if Redis is down the limiter
// fails open and the ledger's own guarantees still hold (INVARIANT 12).
func (s *Server) withRateLimit(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.limiter == nil {
			next.ServeHTTP(w, r)
			return
		}
		allowed, retryAfter := s.limiter.Allow(r.Context(), clientKey(r))
		if !allowed {
			s.metrics.RateLimited.WithLabelValues(route).Inc()
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
			s.writeError(w, r, apperr.New(apperr.CodeRateLimited,
				"too many requests; retry in %s", retryAfter.Round(time.Millisecond)))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientKey identifies the caller for rate-limiting purposes.
func clientKey(r *http.Request) string {
	if key := r.Header.Get("X-Api-Key"); key != "" {
		return "key:" + key
	}
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return "ip:" + fwd
	}
	return "ip:" + r.RemoteAddr
}
