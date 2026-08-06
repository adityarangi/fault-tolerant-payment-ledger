// Command replay republishes historical ledger events to Kafka.
//
// Replay reads outbox history and writes to Kafka. It never opens a ledger
// transaction, so it cannot create entries or change a balance.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/adityarangi/payment-ledger/internal/app"
	"github.com/adityarangi/payment-ledger/internal/kafka"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/outbox"
)

func main() {
	var (
		transactionID = flag.String("transaction", "", "replay events for this transaction ID")
		fromStr       = flag.String("from", "", "replay events created at or after this RFC3339 time")
		toStr         = flag.String("to", "", "replay events created at or before this RFC3339 time")
		limit         = flag.Int("limit", 1000, "maximum number of events to replay")
		dryRun        = flag.Bool("dry-run", false, "list matching events without publishing")
	)
	flag.Parse()

	ctx, cancel := app.SignalContext()
	defer cancel()

	deps, err := app.Build(ctx, "replay")
	if err != nil {
		fail(err)
	}
	defer deps.Close()
	ctx = observability.WithLogger(ctx, deps.Logger)

	req := outbox.ReplayRequest{
		TransactionID: *transactionID,
		Limit:         *limit,
		DryRun:        *dryRun,
		RequestedBy:   "cmd/replay",
	}
	if *fromStr != "" {
		t, err := time.Parse(time.RFC3339, *fromStr)
		if err != nil {
			fail(fmt.Errorf("invalid -from: %w", err))
		}
		req.From = &t
	}
	if *toStr != "" {
		t, err := time.Parse(time.RFC3339, *toStr)
		if err != nil {
			fail(fmt.Errorf("invalid -to: %w", err))
		}
		req.To = &t
	}
	if err := req.Validate(); err != nil {
		fail(err)
	}

	producer := kafka.NewProducer(deps.Config, deps.Metrics)
	defer func() {
		_ = producer.Close()
	}()

	result, err := outbox.NewReplayer(deps.DB.Pool(), producer, deps.Config, deps.Metrics).Replay(ctx, req)
	if err != nil {
		fail(err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(result)

	if result.Failed > 0 {
		os.Exit(1)
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "replay: %v\n", err)
	os.Exit(1)
}
