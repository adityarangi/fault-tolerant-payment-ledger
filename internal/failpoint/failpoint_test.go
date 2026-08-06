package failpoint

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// A disabled registry must be completely inert: this is what keeps failpoints
// out of production behaviour even if the code paths remain compiled in.
func TestDisabledRegistryIsInert(t *testing.T) {
	r := NewRegistry(false)
	if r.Enabled() {
		t.Fatal("registry reports enabled")
	}
	if err := r.Arm(BeforeCommit, "error"); err == nil {
		t.Fatal("a disabled registry armed a failpoint")
	}
	if err := r.Eval(context.Background(), BeforeCommit); err != nil {
		t.Fatalf("disabled registry injected: %v", err)
	}

	var nilRegistry *Registry
	if nilRegistry.Enabled() {
		t.Fatal("nil registry reports enabled")
	}
	if err := nilRegistry.Eval(context.Background(), BeforeCommit); err != nil {
		t.Fatalf("nil registry injected: %v", err)
	}
}

func TestArmAndEvalError(t *testing.T) {
	r := NewRegistry(true)
	if err := r.Arm(BeforeCommit, "error"); err != nil {
		t.Fatalf("arm: %v", err)
	}

	err := r.Eval(context.Background(), BeforeCommit)
	if !errors.Is(err, ErrInjected) {
		t.Fatalf("Eval returned %v, want ErrInjected", err)
	}
	// "error" with no count fires every time.
	if err := r.Eval(context.Background(), BeforeCommit); !errors.Is(err, ErrInjected) {
		t.Fatalf("second Eval returned %v", err)
	}
	// An unarmed failpoint stays silent.
	if err := r.Eval(context.Background(), AfterCommit); err != nil {
		t.Fatalf("unarmed failpoint injected: %v", err)
	}
}

// A bounded count is how a test simulates "fail once, then recover".
func TestArmWithCountExpires(t *testing.T) {
	r := NewRegistry(true)
	if err := r.Arm(AfterKafkaPublish, "error:2"); err != nil {
		t.Fatalf("arm: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := r.Eval(context.Background(), AfterKafkaPublish); !errors.Is(err, ErrInjected) {
			t.Fatalf("call %d returned %v, want an injected error", i, err)
		}
	}
	if err := r.Eval(context.Background(), AfterKafkaPublish); err != nil {
		t.Fatalf("failpoint fired after its budget was exhausted: %v", err)
	}
	if _, ok := r.Active()[AfterKafkaPublish]; ok {
		t.Fatal("exhausted failpoint is still listed as active")
	}
}

func TestParseSpec(t *testing.T) {
	r := NewRegistry(true)
	spec := BeforeCommit + "=error," + AfterCommit + "=error:1," + BeforeKafkaPublish + "=sleep:1ms"
	if err := r.Parse(spec); err != nil {
		t.Fatalf("parse: %v", err)
	}

	active := r.Active()
	if len(active) != 3 {
		t.Fatalf("armed %d failpoints, want 3: %v", len(active), active)
	}
	if err := r.Eval(context.Background(), BeforeCommit); !errors.Is(err, ErrInjected) {
		t.Fatalf("before_commit did not fire: %v", err)
	}

	start := time.Now()
	if err := r.Eval(context.Background(), BeforeKafkaPublish); err != nil {
		t.Fatalf("sleep failpoint returned an error: %v", err)
	}
	if elapsed := time.Since(start); elapsed < time.Millisecond {
		t.Fatalf("sleep failpoint returned after %v", elapsed)
	}
}

func TestParseRejectsUnknownNamesAndActions(t *testing.T) {
	r := NewRegistry(true)
	if err := r.Parse("no_such_failpoint=error"); err == nil {
		t.Fatal("unknown failpoint name accepted")
	}
	if err := r.Arm(BeforeCommit, "explode"); err == nil {
		t.Fatal("unknown action accepted")
	}
	if err := r.Parse("malformed"); err == nil {
		t.Fatal("malformed spec accepted")
	}
	if err := r.Parse(""); err != nil {
		t.Fatalf("empty spec rejected: %v", err)
	}
}

func TestPanicAction(t *testing.T) {
	r := NewRegistry(true)
	if err := r.Arm(BeforeCommit, "panic"); err != nil {
		t.Fatalf("arm: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Error("panic failpoint did not panic")
		}
	}()
	_ = r.Eval(context.Background(), BeforeCommit)
}

// Per-request overrides let one integration test inject a failure without
// affecting requests running concurrently.
func TestRequestScopedOverrides(t *testing.T) {
	r := NewRegistry(true)

	ctx, err := WithRequestFailpoints(context.Background(), BeforeCommit+"=error")
	if err != nil {
		t.Fatalf("build override context: %v", err)
	}
	if err := r.Eval(ctx, BeforeCommit); !errors.Is(err, ErrInjected) {
		t.Fatalf("override did not fire: %v", err)
	}
	// A request without the override is unaffected.
	if err := r.Eval(context.Background(), BeforeCommit); err != nil {
		t.Fatalf("override leaked to another request: %v", err)
	}

	if _, err := WithRequestFailpoints(context.Background(), "bogus=error"); err == nil {
		t.Fatal("unknown failpoint accepted in an override")
	}
}

func TestDisarmAndReset(t *testing.T) {
	r := NewRegistry(true)
	_ = r.Arm(BeforeCommit, "error")
	_ = r.Arm(AfterCommit, "error")

	r.Disarm(BeforeCommit)
	if err := r.Eval(context.Background(), BeforeCommit); err != nil {
		t.Fatalf("disarmed failpoint fired: %v", err)
	}

	r.Reset()
	if len(r.Active()) != 0 {
		t.Fatalf("Reset left %d failpoints armed", len(r.Active()))
	}
}

// The registry is touched from many request goroutines at once; this test is
// meaningful under -race.
func TestConcurrentEvalIsRaceFree(t *testing.T) {
	r := NewRegistry(true)
	_ = r.Arm(BeforeCommit, "error:100")

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = r.Eval(context.Background(), BeforeCommit)
				_ = r.Active()
				_ = r.Arm(AfterCommit, "error:1")
			}
		}()
	}
	wg.Wait()
}
