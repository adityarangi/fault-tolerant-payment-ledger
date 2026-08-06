// Command seed creates the demo accounts and funds them through the public
// API, so the seeded state is produced by exactly the same code path a real
// client would use.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	systemAccount = "system:issuance:usd"
	accountA      = "account-a"
	accountB      = "account-b"
)

func main() {
	baseURL := flag.String("url", envOr("LEDGER_API_URL", "http://localhost:8080"), "ledger API base URL")
	fundAmount := flag.Int64("amount", 100_000, "amount in minor units to fund each account with")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second}
	if err := waitForReady(client, *baseURL); err != nil {
		fail(err)
	}

	// The issuance account is a system account permitted to go negative: it is
	// the counterparty that makes funding a balanced double-entry transaction
	// rather than money appearing from nowhere.
	mustPost(client, *baseURL+"/v1/accounts", "seed-system-account", map[string]any{
		"id": systemAccount, "currency": "USD", "kind": "system", "allow_overdraft": true,
	})
	for _, id := range []string{accountA, accountB} {
		mustPost(client, *baseURL+"/v1/accounts", "seed-"+id, map[string]any{
			"id": id, "currency": "USD", "kind": "user",
		})
	}

	for _, id := range []string{accountA, accountB} {
		mustPost(client, *baseURL+"/v1/transfers", "seed-fund-"+id, map[string]any{
			"source_account_id":      systemAccount,
			"destination_account_id": id,
			"amount":                 *fundAmount,
			"currency":               "USD",
			"description":            "Initial funding",
			"external_reference":     "seed-" + id,
		})
	}

	fmt.Printf("seed: created %s, %s and %s; funded each user account with %d minor units\n",
		systemAccount, accountA, accountB, *fundAmount)
}

func waitForReady(client *http.Client, baseURL string) error {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/health/ready")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("readiness returned %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("api never became ready: %w", lastErr)
}

// mustPost issues an idempotent POST. Re-running the seed is safe: the same
// idempotency keys replay the original responses instead of creating more
// accounts or duplicate funding transactions.
func mustPost(client *http.Client, url, idempotencyKey string, body map[string]any) {
	payload, err := json.Marshal(body)
	if err != nil {
		fail(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		fail(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := client.Do(req)
	if err != nil {
		fail(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	respBody, _ := io.ReadAll(resp.Body)

	switch {
	case resp.StatusCode < 300:
		fmt.Printf("seed: %s -> %d\n", idempotencyKey, resp.StatusCode)
	case resp.StatusCode == http.StatusConflict:
		// Already seeded with a different payload; not fatal for a demo.
		fmt.Printf("seed: %s -> %d (already exists): %s\n", idempotencyKey, resp.StatusCode, respBody)
	default:
		fail(fmt.Errorf("%s returned %d: %s", url, resp.StatusCode, respBody))
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "seed: %v\n", err)
	os.Exit(1)
}
