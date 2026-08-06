package outbox

import (
	"encoding/json"
	"testing"
	"time"
)

// Backoff must grow, stay bounded, and vary — a fixed delay would make every
// event that failed during one outage retry in lockstep.
func TestBackoffIsBoundedAndJittered(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := 2 * time.Second

	for attempt := 1; attempt <= 12; attempt++ {
		for i := 0; i < 50; i++ {
			d := Backoff(attempt, base, maxDelay)
			if d < 0 {
				t.Fatalf("attempt %d: negative delay %v", attempt, d)
			}
			if d > maxDelay {
				t.Fatalf("attempt %d: delay %v exceeds the cap %v", attempt, d, maxDelay)
			}
		}
	}

	// Later attempts should reach further out than the very first one.
	var firstMax, laterMax time.Duration
	for i := 0; i < 200; i++ {
		if d := Backoff(1, base, maxDelay); d > firstMax {
			firstMax = d
		}
		if d := Backoff(5, base, maxDelay); d > laterMax {
			laterMax = d
		}
	}
	if laterMax <= firstMax {
		t.Fatalf("backoff does not grow: attempt 1 max %v, attempt 5 max %v", firstMax, laterMax)
	}

	// Jitter: repeated calls must not all be identical.
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[Backoff(4, base, maxDelay)] = true
	}
	if len(seen) < 2 {
		t.Fatal("backoff produced no jitter")
	}
}

func TestBackoffHandlesDegenerateAttempts(t *testing.T) {
	base := 50 * time.Millisecond
	maxDelay := time.Second
	for _, attempt := range []int{-5, 0, 1} {
		if d := Backoff(attempt, base, maxDelay); d < 0 || d > maxDelay {
			t.Fatalf("attempt %d produced %v", attempt, d)
		}
	}
	// A very large attempt must saturate rather than overflow.
	if d := Backoff(1000, base, maxDelay); d > maxDelay {
		t.Fatalf("attempt 1000 produced %v, want <= %v", d, maxDelay)
	}
}

// A replayed envelope must keep its original event ID, since that is what
// consumers deduplicate on.
func TestMarkReplayedPreservesEventID(t *testing.T) {
	original := Envelope{
		EventID:       "11111111-1111-1111-1111-111111111111",
		EventType:     "payment.created",
		SchemaVersion: SchemaVersion,
		TransactionID: "22222222-2222-2222-2222-222222222222",
		OccurredAt:    time.Now().UTC(),
		Data:          json.RawMessage(`{"amount":2500}`),
	}
	payload, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	meta := ReplayMeta{IsReplay: true, ReplayID: "replay-1", ReplayedAt: time.Now().UTC()}
	replayedRaw, err := markReplayed(payload, meta)
	if err != nil {
		t.Fatalf("markReplayed: %v", err)
	}

	var replayed Envelope
	if err := json.Unmarshal(replayedRaw, &replayed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if replayed.EventID != original.EventID {
		t.Fatalf("event id changed: %q -> %q", original.EventID, replayed.EventID)
	}
	if replayed.TransactionID != original.TransactionID {
		t.Fatalf("transaction id changed: %q -> %q", original.TransactionID, replayed.TransactionID)
	}
	if replayed.Replay == nil || !replayed.Replay.IsReplay {
		t.Fatal("replayed event is not marked as a replay")
	}
	if replayed.Replay.ReplayID != "replay-1" {
		t.Fatalf("replay id = %q, want replay-1", replayed.Replay.ReplayID)
	}
	if string(replayed.Data) != string(original.Data) {
		t.Fatal("replay altered the business payload")
	}
}

func TestMarkReplayedRejectsGarbage(t *testing.T) {
	if _, err := markReplayed([]byte("not json"), ReplayMeta{}); err == nil {
		t.Fatal("expected an error for a malformed envelope")
	}
}

func TestReplayRequestValidate(t *testing.T) {
	// An unbounded replay would republish the entire history by accident.
	if err := (&ReplayRequest{}).Validate(); err == nil {
		t.Fatal("an unbounded replay request was accepted")
	}

	now := time.Now()
	earlier := now.Add(-time.Hour)
	if err := (&ReplayRequest{From: &now, To: &earlier}).Validate(); err == nil {
		t.Fatal("an inverted time range was accepted")
	}
	if err := (&ReplayRequest{TransactionID: "tx-1"}).Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	if err := (&ReplayRequest{TransactionID: "tx-1", Limit: 20001}).Validate(); err == nil {
		t.Fatal("an excessive limit was accepted")
	}
}
