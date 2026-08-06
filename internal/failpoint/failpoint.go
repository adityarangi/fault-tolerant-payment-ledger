// Package failpoint provides named, configurable injection points used by the
// integration tests to prove recovery behaviour.
//
// Failpoints are inert unless explicitly enabled (LEDGER_FAILPOINTS_ENABLED),
// so a production build cannot be tripped by a stray header. Each failpoint is
// evaluated by name and either returns an error, panics, sleeps, or falls
// through.
package failpoint

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Named failpoints. Each corresponds to a specific point in the payment path.
const (
	BeforeTxBegin        = "before_tx_begin"        // before opening the PostgreSQL transaction
	AfterAccountsLocked  = "after_accounts_locked"  // after the balance rows are locked
	AfterEntriesWritten  = "after_entries_written"  // after ledger entries are inserted
	AfterBalancesUpdated = "after_balances_updated" // after balances are updated
	AfterOutboxWritten   = "after_outbox_written"   // after the outbox row is inserted
	BeforeCommit         = "before_commit"          // immediately before COMMIT
	AfterCommit          = "after_commit"           // committed, but before the HTTP response
	BeforeKafkaPublish   = "before_kafka_publish"   // before producing to Kafka
	AfterKafkaPublish    = "after_kafka_publish"    // published, but before marking the outbox row
	BeforeWebhookPersist = "before_webhook_persist" // before webhook delivery state is persisted
	BeforeWebhookSend    = "before_webhook_send"    // before the outbound webhook HTTP call
)

// All lists every known failpoint, used for validation and documentation.
var All = []string{
	BeforeTxBegin, AfterAccountsLocked, AfterEntriesWritten, AfterBalancesUpdated,
	AfterOutboxWritten, BeforeCommit, AfterCommit, BeforeKafkaPublish,
	AfterKafkaPublish, BeforeWebhookPersist, BeforeWebhookSend,
}

// Failpoint actions.
const (
	actionError = "error"
	actionPanic = "panic"
	actionSleep = "sleep"
)

// ErrInjected is returned by a failpoint configured with the "error" action.
var ErrInjected = errors.New("failpoint: injected failure")

type action struct {
	kind     string // error | panic | sleep
	duration time.Duration
	// remaining counts down; -1 means "always".
	remaining int
}

// Registry holds the active failpoint configuration. The zero value is a
// disabled registry that never injects anything.
type Registry struct {
	enabled bool

	mu      sync.Mutex
	actions map[string]*action
}

// NewRegistry builds a registry. When enabled is false every Eval is a no-op
// and the configuration endpoints refuse to arm anything.
func NewRegistry(enabled bool) *Registry {
	return &Registry{enabled: enabled, actions: make(map[string]*action)}
}

// Enabled reports whether failpoint injection is permitted at all.
func (r *Registry) Enabled() bool { return r != nil && r.enabled }

// Parse arms failpoints from a spec string such as
// "before_commit=error,after_commit=error:1,before_kafka_publish=sleep:250ms".
// The optional trailing count limits how many times the action fires.
func (r *Registry) Parse(spec string) error {
	if r == nil || strings.TrimSpace(spec) == "" {
		return nil
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, rhs, ok := strings.Cut(part, "=")
		if !ok {
			return fmt.Errorf("failpoint: malformed spec %q", part)
		}
		if err := r.Arm(strings.TrimSpace(name), strings.TrimSpace(rhs)); err != nil {
			return err
		}
	}
	return nil
}

// Arm configures a single failpoint. spec is "error", "panic", "sleep:100ms",
// optionally suffixed with ":<count>" for error/panic.
func (r *Registry) Arm(name, spec string) error {
	if !r.Enabled() {
		return errors.New("failpoint: injection is disabled")
	}
	if !isKnown(name) {
		return fmt.Errorf("failpoint: unknown failpoint %q", name)
	}

	kind, rest, _ := strings.Cut(spec, ":")
	act := &action{kind: kind, remaining: -1}

	switch kind {
	case actionError, actionPanic:
		if rest != "" {
			n, err := strconv.Atoi(rest)
			if err != nil || n < 0 {
				return fmt.Errorf("failpoint: bad count %q for %s", rest, name)
			}
			act.remaining = n
		}
	case actionSleep:
		d, err := time.ParseDuration(rest)
		if err != nil {
			return fmt.Errorf("failpoint: bad duration %q for %s", rest, name)
		}
		act.duration = d
	case "off", "":
		r.Disarm(name)
		return nil
	default:
		return fmt.Errorf("failpoint: unknown action %q", kind)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions[name] = act
	return nil
}

// Disarm removes a failpoint.
func (r *Registry) Disarm(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.actions, name)
}

// Reset removes every armed failpoint.
func (r *Registry) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = make(map[string]*action)
}

// Active returns the currently armed failpoints for introspection.
func (r *Registry) Active() map[string]string {
	out := make(map[string]string)
	if r == nil {
		return out
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, act := range r.actions {
		desc := act.kind
		if act.kind == actionSleep {
			desc += ":" + act.duration.String()
		} else if act.remaining >= 0 {
			desc += ":" + strconv.Itoa(act.remaining)
		}
		out[name] = desc
	}
	return out
}

// Eval evaluates a failpoint. It returns ErrInjected (wrapped with the name)
// when the failpoint is armed with "error", panics for "panic", and sleeps for
// "sleep". Per-request overrides on the context take precedence over the
// process-wide configuration.
func (r *Registry) Eval(ctx context.Context, name string) error {
	if !r.Enabled() {
		return nil
	}
	if act, ok := fromContext(ctx, name); ok {
		return apply(name, act)
	}

	r.mu.Lock()
	act, ok := r.actions[name]
	if ok && act.remaining > 0 {
		act.remaining--
		if act.remaining == 0 {
			delete(r.actions, name)
		}
	} else if ok && act.remaining == 0 {
		delete(r.actions, name)
		ok = false
	}
	var snapshot action
	if ok {
		snapshot = *act
	}
	r.mu.Unlock()

	if !ok {
		return nil
	}
	return apply(name, &snapshot)
}

func apply(name string, act *action) error {
	switch act.kind {
	case actionError:
		return fmt.Errorf("%w at %s", ErrInjected, name)
	case actionPanic:
		panic("failpoint: injected panic at " + name)
	case actionSleep:
		time.Sleep(act.duration)
	}
	return nil
}

func isKnown(name string) bool {
	for _, n := range All {
		if n == name {
			return true
		}
	}
	return false
}

type ctxKey struct{}

// WithRequestFailpoints attaches per-request failpoint overrides, parsed from
// the X-Failpoint header. Only honoured when the registry is enabled.
func WithRequestFailpoints(ctx context.Context, spec string) (context.Context, error) {
	overrides := make(map[string]*action)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, rhs, ok := strings.Cut(part, "=")
		if !ok {
			return ctx, fmt.Errorf("failpoint: malformed override %q", part)
		}
		name = strings.TrimSpace(name)
		if !isKnown(name) {
			return ctx, fmt.Errorf("failpoint: unknown failpoint %q", name)
		}
		kind, rest, _ := strings.Cut(strings.TrimSpace(rhs), ":")
		act := &action{kind: kind, remaining: -1}
		if kind == actionSleep {
			d, err := time.ParseDuration(rest)
			if err != nil {
				return ctx, fmt.Errorf("failpoint: bad duration %q", rest)
			}
			act.duration = d
		} else if kind != actionError && kind != actionPanic {
			return ctx, fmt.Errorf("failpoint: unknown action %q", kind)
		}
		overrides[name] = act
	}
	if len(overrides) == 0 {
		return ctx, nil
	}
	return context.WithValue(ctx, ctxKey{}, overrides), nil
}

func fromContext(ctx context.Context, name string) (*action, bool) {
	overrides, ok := ctx.Value(ctxKey{}).(map[string]*action)
	if !ok {
		return nil, false
	}
	act, ok := overrides[name]
	return act, ok
}
