// Package api exposes the ledger over HTTP.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/adityarangi/payment-ledger/internal/apperr"
	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/idempotency"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/ledger"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/outbox"
	"github.com/adityarangi/payment-ledger/internal/reconciliation"
	"github.com/adityarangi/payment-ledger/internal/storage"
)

// Server holds the API dependencies.
type Server struct {
	cfg        *config.Config
	db         *storage.DB
	ledger     *ledger.Service
	cache      *idempotency.Cache
	limiter    *RateLimiter
	replayer   *outbox.Replayer
	reconciler *reconciliation.Reconciler
	producer   *kafka.Producer
	redis      *redis.Client
	metrics    *observability.Metrics
	failpoints *failpoint.Registry
	logger     *slog.Logger
}

// Options bundles everything the server needs.
type Options struct {
	Config     *config.Config
	DB         *storage.DB
	Ledger     *ledger.Service
	Cache      *idempotency.Cache
	Redis      *redis.Client
	Replayer   *outbox.Replayer
	Reconciler *reconciliation.Reconciler
	Producer   *kafka.Producer
	Metrics    *observability.Metrics
	Failpoints *failpoint.Registry
	Logger     *slog.Logger
}

// NewServer builds the API server.
func NewServer(opts Options) *Server {
	s := &Server{
		cfg:        opts.Config,
		db:         opts.DB,
		ledger:     opts.Ledger,
		cache:      opts.Cache,
		redis:      opts.Redis,
		replayer:   opts.Replayer,
		reconciler: opts.Reconciler,
		producer:   opts.Producer,
		metrics:    opts.Metrics,
		failpoints: opts.Failpoints,
		logger:     opts.Logger,
	}
	if opts.Config.RateLimitEnabled {
		s.limiter = NewRateLimiter(opts.Redis, opts.Config.RateLimitRPS, time.Second,
			opts.Config.RedisTimeout, opts.Metrics)
	}
	return s
}

// Handler builds the HTTP routing tree.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Reads.
	s.route(mux, "GET /v1/accounts/{id}", s.handleGetAccount, false)
	s.route(mux, "GET /v1/accounts/{id}/balance", s.handleGetBalance, false)
	s.route(mux, "GET /v1/accounts/{id}/entries", s.handleListEntries, false)
	s.route(mux, "GET /v1/transactions/{id}", s.handleGetTransaction, false)

	// Mutations. Every one of these requires an Idempotency-Key.
	s.route(mux, "POST /v1/accounts", s.handleCreateAccount, true)
	s.route(mux, "POST /v1/transfers", s.handleTransfer, true)
	s.route(mux, "POST /v1/transactions/{id}/reverse", s.handleReverse, true)
	s.route(mux, "POST /v1/replay", s.handleReplay, true)

	// Operational endpoints, deliberately unmetered and unlimited.
	mux.Handle("GET /health/live", http.HandlerFunc(s.handleLive))
	mux.Handle("GET /health/ready", http.HandlerFunc(s.handleReady))
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.Handle("GET /v1/reconciliation", s.withRequestContext(http.HandlerFunc(s.handleReconcile)))

	if s.failpoints.Enabled() {
		// Test-only surface; absent unless LEDGER_FAILPOINTS_ENABLED is set.
		mux.Handle("GET /debug/failpoints", s.withRequestContext(http.HandlerFunc(s.handleListFailpoints)))
		mux.Handle("POST /debug/failpoints", s.withRequestContext(http.HandlerFunc(s.handleArmFailpoint)))
		mux.Handle("DELETE /debug/failpoints", s.withRequestContext(http.HandlerFunc(s.handleResetFailpoints)))
	}

	return s.withRecovery(mux)
}

// route wires one handler with the standard middleware chain.
func (s *Server) route(mux *http.ServeMux, pattern string, h http.HandlerFunc, rateLimited bool) {
	var handler http.Handler = h
	handler = s.withFailpoints(handler)
	if rateLimited {
		handler = s.withRateLimit(pattern, handler)
	}
	handler = s.withObservability(pattern, handler)
	handler = s.withRequestContext(handler)
	mux.Handle(pattern, handler)
}

// errorResponse is the stable error envelope returned to clients.
type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code      apperr.Code    `json:"code"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
}

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		observability.Logger(r.Context()).Error("failed to write response", "error", err.Error())
	}
}

// writeRaw replays a stored response body verbatim, which is what makes an
// idempotent retry byte-identical to the original.
func (s *Server) writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	appErr := apperr.From(err)
	status := appErr.HTTPStatus()

	logger := observability.Logger(r.Context())
	if status >= 500 {
		logger.Error("request failed", "code", appErr.Code, "error", appErr.Error())
	} else {
		logger.Warn("request rejected", "code", appErr.Code, "error", appErr.Error())
	}

	// Record the application code for the metrics label when possible.
	if rec, ok := w.(*statusRecorder); ok {
		rec.code = string(appErr.Code)
	}

	s.writeJSON(w, r, status, errorResponse{Error: errorBody{
		Code:      appErr.Code,
		Message:   appErr.Message,
		Details:   appErr.Details,
		RequestID: observability.RequestID(r.Context()),
	}})
}
