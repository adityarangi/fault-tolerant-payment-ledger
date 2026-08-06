# Architecture

## Design premise

A payment system fails in three interesting ways: the database rejects work, a
downstream dependency disappears, or a response is lost after the work already
happened. This design makes the first safe by construction, the second
non-blocking, and the third indistinguishable from success on retry.

The organising rule: **PostgreSQL is the only thing that has to be right.**
Kafka and Redis improve reach and latency; neither can make the ledger wrong.

## Components

| Binary | Responsibility |
|---|---|
| `cmd/api` | HTTP API, idempotency, the payment transaction |
| `cmd/outbox-worker` | Drains `outbox_events` to Kafka with retries and dead-lettering |
| `cmd/webhook-worker` | Consumes payment events, persists delivery intent, delivers with retries |
| `cmd/replay` | Republishes historical events to Kafka |
| `cmd/migrate` | Applies/reverts schema migrations |
| `cmd/seed` | Creates and funds demo accounts through the public API |
| `cmd/example-webhook` | Local receiver that dedupes on event ID and can fail on demand |

| Package | Responsibility |
|---|---|
| `internal/ledger` | Double-entry model and the atomic payment transaction |
| `internal/storage` | Connection pool, migrations, retry-aware transaction helper |
| `internal/idempotency` | Durable records (PostgreSQL) + optional cache (Redis) |
| `internal/outbox` | Outbox rows, publisher, replayer |
| `internal/kafka` | Producer and consumer-group wrappers |
| `internal/webhook` | Delivery state machine and HTTP sender |
| `internal/reconciliation` | Recomputes balances from entries |
| `internal/api` | Routing, middleware, error mapping |
| `internal/observability` | Structured logging, IDs, Prometheus metrics |
| `internal/failpoint` | Named injection points for recovery tests |
| `internal/apperr` | Stable error codes and HTTP status mapping |
| `internal/config` | Environment configuration |
| `internal/app` | Shared dependency wiring |

## Data model

```
accounts ──1:1── balances
    │                 amount BIGINT, CHECK (allow_overdraft OR amount >= 0)
    │
    └──< ledger_entries >── ledger_transactions
              amount BIGINT (signed)      kind: transfer | funding | reversal
              UNIQUE(transaction_id,seq)  reverses_transaction_id ──┐
              composite FKs pin currency                            │
                                                                    │
reversals ──────────────────────────────────────────────────────────┘
    UNIQUE(original_transaction_id)

idempotency_records   PK (scope, key), response_body BYTEA
outbox_events         status, attempts, next_attempt_at, claimed_by
webhook_deliveries    UNIQUE (event_id, endpoint)
```

Two schema decisions carry most of the weight:

**Composite foreign keys pin currency.** `ledger_entries` references
`ledger_transactions (id, currency)` *and* `accounts (id, currency)`. A
cross-currency posting is therefore a foreign-key violation, not a code review
finding. One currency per transaction is structural.

**Balances live in their own table.** The hot row lock taken during a transfer
contends only with other balance writers, never with account metadata reads.
`allow_overdraft` is denormalised onto `balances` so the non-negative rule can
be a plain `CHECK` rather than a trigger.

## The payment path

```
POST /v1/transfers
  ├─ middleware: request/correlation IDs, metrics, rate limit, failpoints
  ├─ require Idempotency-Key, decode, hash the canonical request
  ├─ Redis: cached response for this key?  ──yes──▶ replay, done
  └─ ledger.Transfer
       BEGIN
         advisory lock on the key
         claim the idempotency record  ──existing──▶ replay or 409 conflict
         lock balances, ascending account-ID order
         validate currency and funds
         insert transaction + entries
         update balances (same sorted order)
         insert the outbox event
         complete the idempotency record with the exact response bytes
       COMMIT   ← deferred zero-sum trigger fires
  └─ write the response to Redis, return it
```

Failpoints sit at each boundary so tests can cut the path anywhere and assert
what survives.

## Why the outbox

Writing to Kafka inside the database transaction is impossible to do atomically:
either the commit succeeds and the publish fails (a lost event) or the publish
succeeds and the commit fails (a phantom event). Writing the event to the
*database* in the same transaction removes the choice — the event and the
payment share a fate.

A separate worker then moves events to Kafka at its own pace, with durable
attempt counts. Kafka being down degrades event latency and nothing else.

## Delivery semantics

- **Kafka: at-least-once.** A crash between publishing and marking the row
  republishes the event.
- **Ledger effects: exactly-once.** Guaranteed by PostgreSQL transactions and
  idempotency, not by the broker.
- **Consumers: idempotent.** Deduplication is on `event_id`, which is stable
  across retries and replays.

## Scaling

`cmd/api` is stateless — scale horizontally. Outbox and webhook workers claim
rows with `FOR UPDATE SKIP LOCKED`, so replicas take disjoint work without
coordination; a dead worker's claim lapses after a TTL and is retried by
another. Kafka partitioning is keyed by transaction ID, so events for one
transaction stay ordered relative to each other.

The scaling ceiling is the single PostgreSQL primary. Sharding by account would
be the next step, and would require cross-shard transfers to become a
two-phase protocol — a substantially different design.
