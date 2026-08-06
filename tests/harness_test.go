//go:build integration

// Package tests contains the integration, concurrency and recovery suites.
//
// They run against real PostgreSQL, Kafka and Redis instances (see
// docker-compose.yml), because the properties being verified — row locking,
// deferred constraints, consumer groups, cache fallback — do not exist in a
// mock.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/adityarangi/payment-ledger/internal/api"
	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/idempotency"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/ledger"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/outbox"
	"github.com/adityarangi/payment-ledger/internal/reconciliation"
	"github.com/adityarangi/payment-ledger/internal/storage"
	"github.com/adityarangi/payment-ledger/internal/webhook"
)

// harness is a fully wired ledger stack backed by real infrastructure.
type harness struct {
	t          *testing.T
	cfg        *config.Config
	db         *storage.DB
	redis      *redis.Client
	metrics    *observability.Metrics
	failpoints *failpoint.Registry
	ledger     *ledger.Service
	producer   *kafka.Producer
	replayer   *outbox.Replayer
	reconciler *reconciliation.Reconciler
	server     *httptest.Server
	client     *http.Client
}

// kafkaAvailable reports whether a broker is reachable, so Kafka-dependent
// tests can skip cleanly rather than hang.
func kafkaAvailable(cfg *config.Config) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return kafka.EnsureTopics(ctx, cfg.KafkaBrokers, 1, cfg.AllTopics()...) == nil
}

// newHarness builds an isolated stack. Each harness gets its own PostgreSQL
// schema so tests never see each other's rows and can run in parallel.
func newHarness(t *testing.T, opts ...func(*config.Config)) *harness {
	t.Helper()
	if os.Getenv("LEDGER_TEST_INTEGRATION") == "" {
		t.Skip("set LEDGER_TEST_INTEGRATION=1 to run integration tests")
	}

	cfg, err := config.Load("ledger-test")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	cfg.LogLevel = "error"
	cfg.RateLimitEnabled = false
	cfg.FailpointsEnabled = true
	cfg.OutboxPollInterval = 50 * time.Millisecond
	cfg.WebhookPollInterval = 50 * time.Millisecond
	// A short claim TTL keeps the "crashed worker" recovery path fast: it is
	// the interval after which another worker may reclaim an abandoned row.
	cfg.OutboxClaimTTL = 500 * time.Millisecond
	cfg.OutboxBaseBackoff = 10 * time.Millisecond
	cfg.OutboxMaxBackoff = 200 * time.Millisecond
	cfg.WebhookBaseBackoff = 10 * time.Millisecond
	cfg.WebhookMaxBackoff = 200 * time.Millisecond
	for _, opt := range opts {
		opt(cfg)
	}

	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24]
	cfg.DatabaseURL = withSearchPath(cfg.DatabaseURL, schema)

	ctx := context.Background()
	if err := createSchema(ctx, cfg.DatabaseURL, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	metrics := observability.NewMetrics()
	db, err := storage.Open(ctx, cfg, metrics)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	if _, err := storage.MigrateUp(ctx, db.Pool()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var rdb *redis.Client
	if cfg.RedisEnabled {
		rdb = redis.NewClient(&redis.Options{
			Addr:         cfg.RedisAddr,
			DB:           cfg.RedisDB,
			DialTimeout:  cfg.RedisTimeout,
			ReadTimeout:  cfg.RedisTimeout,
			WriteTimeout: cfg.RedisTimeout,
		})
	}

	fp := failpoint.NewRegistry(true)
	logger := observability.NewLogger("ledger-test", cfg.LogLevel)
	producer := kafka.NewProducer(cfg, metrics)
	svc := ledger.NewService(db, cfg, metrics, fp)
	replayer := outbox.NewReplayer(db.Pool(), producer, cfg, metrics)
	reconciler := reconciliation.New(db.Pool(), metrics)

	server := api.NewServer(api.Options{
		Config:     cfg,
		DB:         db,
		Ledger:     svc,
		Cache:      idempotency.NewCache(rdb, cfg.IdempotencyCacheTTL, cfg.RedisTimeout, metrics),
		Redis:      rdb,
		Replayer:   replayer,
		Reconciler: reconciler,
		Producer:   producer,
		Metrics:    metrics,
		Failpoints: fp,
		Logger:     logger,
	})

	h := &harness{
		t: t, cfg: cfg, db: db, redis: rdb, metrics: metrics, failpoints: fp,
		ledger: svc, producer: producer, replayer: replayer, reconciler: reconciler,
		server: httptest.NewServer(server.Handler()),
		client: &http.Client{Timeout: 30 * time.Second},
	}

	t.Cleanup(func() {
		h.server.Close()
		_ = producer.Close()
		db.Close()
		if rdb != nil {
			_ = rdb.Close()
		}
		dropSchema(cfg.DatabaseURL, schema)
	})
	return h
}

func withSearchPath(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}

func createSchema(ctx context.Context, dsn, schema string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema)
	return err
}

func dropSchema(dsn, schema string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
}

// --- HTTP helpers ---

type apiResponse struct {
	Status  int
	Body    []byte
	Headers http.Header
}

func (r apiResponse) decode(t *testing.T, dst any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, dst); err != nil {
		t.Fatalf("decode response %s: %v", r.Body, err)
	}
}

func (r apiResponse) errorCode(t *testing.T) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	r.decode(t, &body)
	return body.Error.Code
}

// do issues a request against the harness API.
func (h *harness) do(method, path string, body any, headers map[string]string) apiResponse {
	h.t.Helper()

	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(method, h.server.URL+path, reader)
	if err != nil {
		h.t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		h.t.Fatalf("read response: %v", err)
	}
	return apiResponse{Status: resp.StatusCode, Body: respBody, Headers: resp.Header}
}

func (h *harness) post(path, idempotencyKey string, body any) apiResponse {
	return h.do(http.MethodPost, path, body, map[string]string{"Idempotency-Key": idempotencyKey})
}

func (h *harness) postWithFailpoint(path, idempotencyKey, failpointSpec string, body any) apiResponse {
	return h.do(http.MethodPost, path, body, map[string]string{
		"Idempotency-Key":   idempotencyKey,
		api.HeaderFailpoint: failpointSpec,
	})
}

func (h *harness) get(path string) apiResponse {
	return h.do(http.MethodGet, path, nil, nil)
}

// --- domain helpers ---

func (h *harness) createAccount(id, currency, kind string, overdraft bool) {
	h.t.Helper()
	resp := h.post("/v1/accounts", "create-"+id+"-"+uuid.NewString(), map[string]any{
		"id": id, "currency": currency, "kind": kind, "allow_overdraft": overdraft,
	})
	if resp.Status != http.StatusCreated {
		h.t.Fatalf("create account %s: status %d body %s", id, resp.Status, resp.Body)
	}
}

// newAccounts creates a funded user account plus a system issuance account.
func (h *harness) newAccounts(currency string) (system, a, b string) {
	h.t.Helper()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	system = "system-" + suffix
	a = "acct-a-" + suffix
	b = "acct-b-" + suffix

	h.createAccount(system, currency, ledger.KindSystem, true)
	h.createAccount(a, currency, ledger.KindUser, false)
	h.createAccount(b, currency, ledger.KindUser, false)
	return system, a, b
}

// fund moves money from a system account, which keeps funding a balanced
// double-entry transaction rather than a balance edit.
func (h *harness) fund(system, account string, amount int64, currency string) {
	h.t.Helper()
	resp := h.post("/v1/transfers", "fund-"+account+"-"+uuid.NewString(), map[string]any{
		"source_account_id":      system,
		"destination_account_id": account,
		"amount":                 amount,
		"currency":               currency,
		"description":            "test funding",
	})
	if resp.Status != http.StatusCreated {
		h.t.Fatalf("fund %s: status %d body %s", account, resp.Status, resp.Body)
	}
}

func (h *harness) balance(account string) int64 {
	h.t.Helper()
	resp := h.get("/v1/accounts/" + account + "/balance")
	if resp.Status != http.StatusOK {
		h.t.Fatalf("balance %s: status %d body %s", account, resp.Status, resp.Body)
	}
	var b ledger.Balance
	resp.decode(h.t, &b)
	return b.Amount
}

func (h *harness) transfer(key, from, to string, amount int64, currency string) apiResponse {
	return h.post("/v1/transfers", key, map[string]any{
		"source_account_id":      from,
		"destination_account_id": to,
		"amount":                 amount,
		"currency":               currency,
	})
}

// countTransactions returns how many ledger transactions exist, used to prove
// that a retried idempotency key created at most one.
func (h *harness) countTransactions(ctx context.Context) int {
	h.t.Helper()
	var n int
	if err := h.db.Pool().QueryRow(ctx, `SELECT count(*) FROM ledger_transactions`).Scan(&n); err != nil {
		h.t.Fatalf("count transactions: %v", err)
	}
	return n
}

func (h *harness) countEntries(ctx context.Context) int {
	h.t.Helper()
	var n int
	if err := h.db.Pool().QueryRow(ctx, `SELECT count(*) FROM ledger_entries`).Scan(&n); err != nil {
		h.t.Fatalf("count entries: %v", err)
	}
	return n
}

func (h *harness) countOutbox(ctx context.Context, status string) int {
	h.t.Helper()
	var n int
	query := `SELECT count(*) FROM outbox_events`
	args := []any{}
	if status != "" {
		query += ` WHERE status = $1`
		args = append(args, status)
	}
	if err := h.db.Pool().QueryRow(ctx, query, args...).Scan(&n); err != nil {
		h.t.Fatalf("count outbox: %v", err)
	}
	return n
}

// requireReconciled asserts that balances still agree with the entries.
func (h *harness) requireReconciled(ctx context.Context) {
	h.t.Helper()
	report, err := h.reconciler.Run(ctx)
	if err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
	if !report.OK() {
		h.t.Fatalf("ledger inconsistent: mismatches=%+v unbalanced=%v",
			report.Mismatches, report.UnbalancedTxIDs)
	}
}

// newPublisher builds an outbox publisher bound to this harness, simulating a
// separately deployed worker process.
func (h *harness) newPublisher() *outbox.Publisher {
	return outbox.NewPublisher(h.db.Pool(), h.producer, h.cfg, h.metrics, h.failpoints)
}

func (h *harness) newWebhookWorker() *webhook.Worker {
	return webhook.NewWorker(h.db.Pool(), h.cfg, h.metrics, h.failpoints)
}

// eventually polls until cond holds or the timeout expires.
func eventually(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}
