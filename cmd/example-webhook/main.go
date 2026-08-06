// Command example-webhook is a local webhook receiver for the demo.
//
// It records the event IDs it has processed and ignores duplicates, which is
// exactly what a real consumer must do given at-least-once Kafka delivery and
// webhook retries. It can also be told to fail on demand so retry and
// dead-letter behaviour can be exercised end to end.
package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// keyStatus is the JSON field this receiver reports its outcome under.
const keyStatus = "status"

// receiver tracks processed event IDs so repeated deliveries are no-ops.
type receiver struct {
	mu         sync.Mutex
	processed  map[string]int // event ID -> times delivered
	order      []string
	failNext   int // fail this many upcoming deliveries
	failAlways bool
	logger     *slog.Logger
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "example-webhook")

	rec := &receiver{processed: map[string]int{}, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", rec.handleWebhook)
	mux.HandleFunc("GET /events", rec.handleEvents)
	mux.HandleFunc("POST /fail", rec.handleFail)
	mux.HandleFunc("POST /reset", rec.handleReset)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{keyStatus: "ok"})
	})

	addr := os.Getenv("EXAMPLE_WEBHOOK_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	logger.Info("example webhook receiver listening", "addr", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server failed", "error", err.Error())
		os.Exit(1)
	}
}

func (r *receiver) handleWebhook(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unreadable body"})
		return
	}

	var envelope struct {
		EventID       string `json:"event_id"`
		EventType     string `json:"event_type"`
		TransactionID string `json:"transaction_id"`
		Replay        *struct {
			IsReplay bool   `json:"is_replay"`
			ReplayID string `json:"replay_id"`
		} `json:"replay"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.EventID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing event_id"})
		return
	}

	r.mu.Lock()
	if r.failAlways || r.failNext > 0 {
		if r.failNext > 0 {
			r.failNext--
		}
		remaining := r.failNext
		r.mu.Unlock()
		r.logger.Warn("simulating webhook failure",
			"event_id", envelope.EventID, "remaining_failures", remaining)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "simulated failure"})
		return
	}

	count := r.processed[envelope.EventID]
	r.processed[envelope.EventID] = count + 1
	if count == 0 {
		r.order = append(r.order, envelope.EventID)
	}
	r.mu.Unlock()

	isReplay := envelope.Replay != nil && envelope.Replay.IsReplay
	if count > 0 {
		// The event ID has been seen before, so this delivery does no new
		// work. Acknowledging with 200 is what stops the sender retrying.
		r.logger.Info("duplicate event ignored",
			"event_id", envelope.EventID, "deliveries", count+1, "replay", isReplay)
		writeJSON(w, http.StatusOK, map[string]any{keyStatus: "duplicate_ignored", "event_id": envelope.EventID})
		return
	}

	r.logger.Info("processed event",
		"event_id", envelope.EventID, "event_type", envelope.EventType,
		"transaction_id", envelope.TransactionID, "replay", isReplay)
	writeJSON(w, http.StatusOK, map[string]any{keyStatus: "processed", "event_id": envelope.EventID})
}

func (r *receiver) handleEvents(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()

	events := make([]map[string]any, 0, len(r.order))
	for _, id := range r.order {
		events = append(events, map[string]any{"event_id": id, "deliveries": r.processed[id]})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"unique_events": len(r.order),
		"events":        events,
	})
}

// handleFail arms simulated failures: ?count=N for N failures, ?always=true
// to fail until reset.
func (r *receiver) handleFail(w http.ResponseWriter, req *http.Request) {
	count, _ := strconv.Atoi(req.URL.Query().Get("count"))
	always, _ := strconv.ParseBool(req.URL.Query().Get("always"))

	r.mu.Lock()
	r.failNext = count
	r.failAlways = always
	r.mu.Unlock()

	r.logger.Warn("arming simulated failures", "count", count, "always", always)
	writeJSON(w, http.StatusOK, map[string]any{"fail_next": count, "fail_always": always})
}

func (r *receiver) handleReset(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	r.processed = map[string]int{}
	r.order = nil
	r.failNext = 0
	r.failAlways = false
	r.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{keyStatus: "reset"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
