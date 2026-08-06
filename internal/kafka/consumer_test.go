package kafka

import (
	"testing"
	"time"
)

// A broker outage must not kill a consumer. The retry pause has to grow (so a
// long outage does not hammer the broker) and stay bounded (so recovery is not
// delayed for minutes), with jitter so replicas do not reconnect in lockstep.
func TestFetchBackoffGrowsAndIsBounded(t *testing.T) {
	const maxDelay = 30 * time.Second

	for failures := 1; failures <= 20; failures++ {
		for i := 0; i < 25; i++ {
			d := fetchBackoff(failures)
			if d <= 0 {
				t.Fatalf("failures=%d: non-positive delay %v", failures, d)
			}
			if d > maxDelay {
				t.Fatalf("failures=%d: delay %v exceeds the %v cap", failures, d, maxDelay)
			}
		}
	}

	var firstMax, laterMax time.Duration
	for i := 0; i < 200; i++ {
		if d := fetchBackoff(1); d > firstMax {
			firstMax = d
		}
		if d := fetchBackoff(6); d > laterMax {
			laterMax = d
		}
	}
	if laterMax <= firstMax {
		t.Fatalf("backoff does not grow: 1 failure peaked at %v, 6 failures at %v", firstMax, laterMax)
	}

	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[fetchBackoff(3)] = true
	}
	if len(seen) < 2 {
		t.Fatal("fetch backoff produced no jitter")
	}
}

func TestFetchBackoffHandlesDegenerateInput(t *testing.T) {
	for _, failures := range []int{-3, 0, 1} {
		if d := fetchBackoff(failures); d <= 0 || d > 30*time.Second {
			t.Fatalf("failures=%d produced %v", failures, d)
		}
	}
	// A very long outage must saturate rather than overflow into a negative
	// or absurd duration.
	if d := fetchBackoff(10_000); d <= 0 || d > 30*time.Second {
		t.Fatalf("failures=10000 produced %v", d)
	}
}
