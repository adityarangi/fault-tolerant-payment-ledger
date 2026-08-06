package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adityarangi/payment-ledger/internal/apperr"
	"github.com/adityarangi/payment-ledger/internal/idempotency"
	"github.com/adityarangi/payment-ledger/internal/ledger"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/outbox"
)

// maxBodyBytes bounds request bodies; payment payloads are small.
const maxBodyBytes = 1 << 20

// Idempotency scopes. The scope is part of the key so the same key used on
// different endpoints cannot collide.
const (
	scopeCreateAccount = "POST /v1/accounts"
	scopeTransfer      = "POST /v1/transfers"
	scopeReverse       = "POST /v1/transactions/reverse"
	scopeReplay        = "POST /v1/replay"
)

type createAccountRequest struct {
	ID             string         `json:"id"`
	Currency       string         `json:"currency"`
	Kind           string         `json:"kind"`
	AllowOverdraft bool           `json:"allow_overdraft"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type transferRequest struct {
	SourceAccountID      string `json:"source_account_id"`
	DestinationAccountID string `json:"destination_account_id"`
	Amount               int64  `json:"amount"`
	Currency             string `json:"currency"`
	Description          string `json:"description,omitempty"`
	ExternalReference    string `json:"external_reference,omitempty"`
}

type reverseRequest struct {
	Reason string `json:"reason,omitempty"`
	// TransactionID is filled from the path, not the body, but is part of the
	// hashed request so the same key cannot be reused for a different target.
	TransactionID string `json:"transaction_id"`
}

func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	var req createAccountRequest
	if err := decodeJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	if req.Kind == "" {
		req.Kind = ledger.KindUser
	}

	hash, err := idempotency.HashRequest(scopeCreateAccount, req)
	if err != nil {
		s.writeError(w, r, apperr.Internal(err))
		return
	}
	if replayed := s.replayFromCache(w, r, scopeCreateAccount, key, hash); replayed {
		return
	}

	result, err := s.ledger.CreateAccount(r.Context(), ledger.CreateAccountCommand{
		ID:               req.ID,
		Currency:         req.Currency,
		Kind:             req.Kind,
		AllowOverdraft:   req.AllowOverdraft,
		Metadata:         req.Metadata,
		IdempotencyScope: scopeCreateAccount,
		IdempotencyKey:   key,
		RequestHash:      hash,
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.finishIdempotent(w, r, scopeCreateAccount, key, hash, result)
}

func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	var req transferRequest
	if err := decodeJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}

	hash, err := idempotency.HashRequest(scopeTransfer, req)
	if err != nil {
		s.writeError(w, r, apperr.Internal(err))
		return
	}
	// Fast path: a completed response cached in Redis short-circuits the whole
	// database round trip. A miss, a stale entry or a Redis outage simply
	// falls through to PostgreSQL, which is authoritative.
	if replayed := s.replayFromCache(w, r, scopeTransfer, key, hash); replayed {
		return
	}

	result, err := s.ledger.Transfer(r.Context(), ledger.TransferCommand{
		SourceAccountID:      req.SourceAccountID,
		DestinationAccountID: req.DestinationAccountID,
		Amount:               req.Amount,
		Currency:             req.Currency,
		Description:          req.Description,
		ExternalReference:    req.ExternalReference,
		IdempotencyScope:     scopeTransfer,
		IdempotencyKey:       key,
		RequestHash:          hash,
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.finishIdempotent(w, r, scopeTransfer, key, hash, result)
}

func (s *Server) handleReverse(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	var body struct {
		Reason string `json:"reason,omitempty"`
	}
	// A reversal body is optional.
	if r.ContentLength > 0 {
		if err := decodeJSON(r, &body); err != nil {
			s.writeError(w, r, err)
			return
		}
	}

	req := reverseRequest{Reason: body.Reason, TransactionID: r.PathValue("id")}
	hash, err := idempotency.HashRequest(scopeReverse, req)
	if err != nil {
		s.writeError(w, r, apperr.Internal(err))
		return
	}
	if replayed := s.replayFromCache(w, r, scopeReverse, key, hash); replayed {
		return
	}

	result, err := s.ledger.Reverse(r.Context(), ledger.ReverseCommand{
		TransactionID:    req.TransactionID,
		Reason:           req.Reason,
		IdempotencyScope: scopeReverse,
		IdempotencyKey:   key,
		RequestHash:      hash,
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.finishIdempotent(w, r, scopeReverse, key, hash, result)
}

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	acct, err := s.ledger.GetAccount(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, acct)
}

func (s *Server) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	balance, err := s.ledger.GetBalance(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, balance)
}

func (s *Server) handleListEntries(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			s.writeError(w, r, apperr.InvalidRequest("limit must be a positive integer"))
			return
		}
		limit = parsed
	}
	page, err := s.ledger.ListEntries(r.Context(), r.PathValue("id"), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, page)
}

func (s *Server) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	txn, err := s.ledger.GetTransaction(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, txn)
}

// handleReplay republishes historical events to Kafka.
//
// Replay is idempotent at the API level via the durable record, but note it is
// also inherently safe to repeat: it only re-emits existing events under their
// original event IDs, and consumers deduplicate.
func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	key, err := idempotencyKey(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	var req outbox.ReplayRequest
	if err := decodeJSON(r, &req); err != nil {
		s.writeError(w, r, err)
		return
	}
	if err := req.Validate(); err != nil {
		s.writeError(w, r, err)
		return
	}
	req.RequestedBy = observability.RequestID(r.Context())

	hash, err := idempotency.HashRequest(scopeReplay, req)
	if err != nil {
		s.writeError(w, r, apperr.Internal(err))
		return
	}

	if replayed := s.replayFromCache(w, r, scopeReplay, key, hash); replayed {
		return
	}

	// Unlike a transfer, the work here is an external Kafka publish that
	// cannot join a database transaction. So the record is committed as
	// in_progress first and completed afterwards, and a crash in between
	// leaves a key that reports in_progress rather than a false success.
	// This is safe precisely because replay never touches the ledger.
	status, body, err := s.runExternalIdempotent(r, scopeReplay, key, hash,
		func(ctx context.Context) (int, []byte, error) {
			result, err := s.replayer.Replay(ctx, req)
			if err != nil {
				return 0, nil, err
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				return 0, nil, apperr.Internal(err)
			}
			return http.StatusOK, encoded, nil
		})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	s.cache.Put(r.Context(), scopeReplay, key, idempotency.CachedResponse{
		Status: status, Body: body, RequestHash: hash,
	})
	s.writeRaw(w, status, body)
}

// runExternalIdempotent applies durable idempotency to work that cannot run
// inside the ledger transaction. The claim, the work and the completion are
// three separate steps; PostgreSQL still holds the authoritative record.
func (s *Server) runExternalIdempotent(
	r *http.Request, scope, key, hash string,
	work func(ctx context.Context) (int, []byte, error),
) (int, []byte, error) {
	ctx := r.Context()

	var replayStatus int
	var replayBody []byte
	claimed := false

	err := s.db.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := idempotency.LockKey(ctx, tx, scope, key); err != nil {
			return err
		}
		existing, err := idempotency.Claim(ctx, tx, scope, key, hash)
		if err != nil {
			return err
		}
		if existing == nil {
			claimed = true
			return nil
		}
		if existing.RequestHash != hash {
			return idempotency.Conflict(key)
		}
		if existing.State != idempotency.StateCompleted {
			return idempotency.InProgress(key)
		}
		replayStatus, replayBody = existing.ResponseStatus, existing.ResponseBody
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	if !claimed {
		s.metrics.IdempotencyTotal.WithLabelValues("hit", "postgres").Inc()
		return replayStatus, replayBody, nil
	}
	s.metrics.IdempotencyTotal.WithLabelValues("miss", "postgres").Inc()

	status, body, err := work(ctx)
	if err != nil {
		// Release the key so the caller can retry; the work did not complete.
		if derr := idempotency.Discard(context.WithoutCancel(ctx), s.db.Pool(), scope, key); derr != nil {
			observability.Logger(ctx).Error("failed to release idempotency claim",
				"scope", scope, "error", derr.Error())
		}
		return 0, nil, err
	}

	if err := s.db.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return idempotency.Complete(ctx, tx, scope, key, status, body, nil)
	}); err != nil {
		return 0, nil, err
	}
	return status, body, nil
}

func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	report, err := s.reconciler.Run(r.Context())
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if !report.OK() {
		status = http.StatusConflict
	}
	s.writeJSON(w, r, status, report)
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady reports readiness. PostgreSQL is a hard dependency; Redis and
// Kafka are reported but never make the API unready, because the ledger keeps
// working correctly without them.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{"postgres": "ok", "redis": "disabled", "kafka": "unchecked"}
	status := http.StatusOK

	if err := s.db.Ping(ctx); err != nil {
		checks["postgres"] = "error: " + err.Error()
		status = http.StatusServiceUnavailable
	}
	if s.redis != nil {
		if err := s.redis.Ping(ctx).Err(); err != nil {
			// Degraded, not unready.
			checks["redis"] = "degraded: " + err.Error()
		} else {
			checks["redis"] = "ok"
		}
	}

	s.writeJSON(w, r, status, map[string]any{
		"status": map[bool]string{true: "ready", false: "not_ready"}[status == http.StatusOK],
		"checks": checks,
	})
}

// --- idempotency helpers ---

func idempotencyKey(r *http.Request) (string, error) {
	key := r.Header.Get(HeaderIdempotencyKey)
	if key == "" {
		return "", apperr.InvalidRequest("%s header is required for all mutating requests", HeaderIdempotencyKey)
	}
	if len(key) > 255 {
		return "", apperr.InvalidRequest("%s must be at most 255 characters", HeaderIdempotencyKey)
	}
	return key, nil
}

// replayFromCache serves a completed response straight from Redis. It returns
// true when the response has been written.
func (s *Server) replayFromCache(w http.ResponseWriter, r *http.Request, scope, key, hash string) bool {
	cached := s.cache.Get(r.Context(), scope, key)
	if cached == nil {
		return false
	}
	if cached.RequestHash != hash {
		s.metrics.IdempotencyTotal.WithLabelValues("conflict", "redis").Inc()
		s.writeError(w, r, idempotency.Conflict(key))
		return true
	}
	s.metrics.IdempotencyTotal.WithLabelValues("hit", "redis").Inc()
	w.Header().Set(HeaderIdempotentHit, "true")
	s.writeRaw(w, cached.Status, cached.Body)
	return true
}

// finishIdempotent writes the result and populates the Redis cache.
func (s *Server) finishIdempotent(w http.ResponseWriter, r *http.Request, scope, key, hash string, result *ledger.Result) {
	txID := ""
	if result.Transaction != nil {
		txID = result.Transaction.ID
	}
	// Cache after the commit, never before: Redis must only ever hold
	// responses that PostgreSQL has already made durable.
	s.cache.Put(r.Context(), scope, key, idempotency.CachedResponse{
		Status:        result.ResponseStatus,
		Body:          result.ResponseBody,
		RequestHash:   hash,
		TransactionID: txID,
	})
	if result.Replayed {
		w.Header().Set(HeaderIdempotentHit, "true")
	}
	s.writeRaw(w, result.ResponseStatus, result.ResponseBody)
}

func decodeJSON(r *http.Request, dst any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return apperr.InvalidRequest("could not read request body")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return apperr.InvalidRequest("malformed JSON body: %s", err.Error())
	}
	return nil
}
