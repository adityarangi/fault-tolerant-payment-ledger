# Fault-Tolerant Payment Ledger

 Go · PostgreSQL · Kafka · Redis

A double-entry payment ledger built to keep its books correct while the
infrastructure around it misbehaves. Money moves through one atomic PostgreSQL
transaction; events reach the outside world through a transactional outbox and
Kafka; retries, restarts, broker outages and cache failures are exercised by
tests rather than assumed away.

> This is a production-inspired **educational** project, not a real payment
> processor. It makes no claim of PCI compliance and does not implement
> exactly-once distributed messaging. See [Known limitations](#known-limitations).

---

## Architecture

```
                       ┌──────────────────────────────────────────┐
   client ──HTTP──▶    │            cmd/api                       │
   Idempotency-Key     │  validate → idempotency → ledger service │
                       └───────────────┬──────────────────────────┘
                                       │
                    ┌──────────────────▼───────────────────┐
                    │   ONE PostgreSQL transaction         │
                    │  • ledger_transactions               │
                    │  • ledger_entries  (debit + credit)  │
                    │  • balances        (row-locked)      │
                    │  • idempotency_records (durable)     │
                    │  • outbox_events   (the event)       │
                    └──────────────────┬───────────────────┘
                                       │ COMMIT
              ┌────────────────────────┴───────────────────────┐
              │                                                │
     ┌────────▼─────────┐                            ┌─────────▼──────────┐
     │  Redis (cache)   │                            │ cmd/outbox-worker  │
     │  • idempotent    │                            │ claim SKIP LOCKED  │
     │    responses     │                            │ publish + backoff  │
     │  • rate limiting │                            └─────────┬──────────┘
     │  OPTIONAL        │                                      │ at-least-once
     └──────────────────┘                            ┌─────────▼──────────┐
                                                     │       Kafka        │
                                                     │ payments.created   │
                                                     │ payments.reversed  │
                                                     │ payments.failed    │
                                                     │ payments.dlq       │
                                                     └─────────┬──────────┘
                                        ┌──────────────────────┴─────────┐
                                        │      cmd/webhook-worker        │
                                        │ consume → persist intent →     │
                                        │ deliver with retry/dead-letter │
                                        └──────────────────┬─────────────┘
                                                           │
                                                  ┌────────▼─────────┐
                                                  │ cmd/example-     │
                                                  │ webhook receiver │
                                                  │ dedupes event_id │
                                                  └──────────────────┘

     cmd/replay ──▶ reads outbox history ──▶ republishes to Kafka
                    (never opens a ledger transaction)
```

**Who owns what**

| Component  | Role                                            | Correctness dependency? |
|------------|-------------------------------------------------|-------------------------|
| PostgreSQL | Authoritative ledger, balances, idempotency, outbox | **Yes — the source of truth** |
| Kafka      | Event distribution, at-least-once                | No — events survive in the outbox |
| Redis      | Idempotency response cache, rate limiting        | No — pure optimisation |

---

## Double-entry example

A $25.00 transfer is one transaction with two entries that sum to zero. Amounts
are signed integers in **minor units** — never floating point.

```
transaction 7f3c… (kind=transfer, currency=USD)
  ┌──────────────────────────┬──────────┬─────┐
  │ account                  │   amount │ seq │
  ├──────────────────────────┼──────────┼─────┤
  │ account-a                │    -2500 │  0  │  debit
  │ account-b                │    +2500 │  1  │  credit
  ├──────────────────────────┼──────────┼─────┤
  │ SUM                      │        0 │     │  ← enforced by PostgreSQL
  └──────────────────────────┴──────────┴─────┘
```

Funding is not a balance edit — it is a balanced transfer from a system account
that is permitted to go negative:

```
system:issuance:usd   -100000     (a system account may run negative)
account-a             +100000
                    ─────────
                            0
```

An account's balance always equals the sum of its entries, so the whole ledger
sums to zero. Nothing is ever edited: a mistake is corrected by a **reversal**,
a new transaction with mirrored entries that leaves the original intact.

---

## PostgreSQL transaction flow

A successful transfer commits all of this atomically, or none of it:

```
BEGIN  (READ COMMITTED)
  ├─ pg_advisory_xact_lock(idempotency key)     serialise same-key retries
  ├─ INSERT idempotency_records … ON CONFLICT   claim the key, or replay
  ├─ SELECT balances FOR NO KEY UPDATE          locked in ascending account-ID order
  ├─ validate currency + funds
  ├─ INSERT ledger_transactions
  ├─ INSERT ledger_entries  (debit, credit)     deferred zero-sum trigger armed
  ├─ UPDATE balances        (same sorted order) CHECK (amount >= 0) is the real guard
  ├─ INSERT outbox_events                       the event, in the same transaction
  └─ UPDATE idempotency_records → completed     stores the exact response bytes
COMMIT                                          ← zero-sum trigger fires here
```

**Concurrency strategy**

- **Deterministic lock ordering.** Balance rows are always locked in ascending
  account-ID order, so opposing transfers queue instead of forming a cycle.
- **Row-level locks.** `SELECT … FOR NO KEY UPDATE` on `balances` only; account
  metadata reads never contend with payments.
- **Database-enforced invariants.** `CHECK (allow_overdraft OR amount >= 0)`
  makes overdraft impossible even if the Go check were wrong. A deferred
  constraint trigger validates the zero-sum rule at `COMMIT`. Triggers reject
  any `UPDATE`/`DELETE` on ledger rows.
- **Bounded waits + retries.** `lock_timeout` and `statement_timeout` turn a
  potential hang into a retryable error; deadlock (40P01), serialization
  (40001) and lock-timeout errors are retried with exponential backoff and full
  jitter.

---

## Idempotency flow

Every mutating endpoint requires an `Idempotency-Key` header.

```
   request + key
        │
        ▼
  ┌─────────────────┐   hit, same request    ┌────────────────────────┐
  │ Redis cache     │───────────────────────▶│ replay stored response │
  │ (optional)      │                        └────────────────────────┘
  └────────┬────────┘   miss / stale / Redis down
           ▼
  ┌──────────────────────────────────────────────────────────┐
  │ PostgreSQL (authoritative)                               │
  │  key absent          → execute, store response, commit   │
  │  key + same hash     → replay the original response      │
  │  key + different hash→ 409 idempotency_conflict          │
  └──────────────────────────────────────────────────────────┘
```

- The idempotency record is written **in the same transaction as the payment**,
  so "at most one ledger transaction per key" is a database guarantee.
- Responses are stored as raw bytes (`BYTEA`, not `JSONB`) so a retry returns a
  **byte-identical** response, not merely an equivalent one.
- A request that fails mid-transaction rolls the record back too, so the key
  stays usable — nothing was committed, so nothing needs replaying.
- Concurrent requests sharing a key are serialised by a transaction-scoped
  advisory lock; the `(scope, key)` primary key is the real guarantee, so a hash
  collision costs only a little extra serialisation.

---

## Transactional outbox flow

The event and the payment are written together, so neither can exist alone.

```
  payment transaction ──COMMIT──▶ outbox_events(status=pending)
                                          │
                            ┌─────────────▼─────────────┐
                            │ outbox-worker (N replicas)│
                            │ SELECT … FOR UPDATE       │
                            │        SKIP LOCKED        │  ← replicas never collide
                            └─────────────┬─────────────┘
                                          │ publish (acks=all)
                    success ──────────────┼────────────── failure
                        │                                    │
                 status=published                   attempts++, last_error,
                                                    next_attempt_at = now + backoff
                                                            │
                                              attempts >= max → dead_letter
                                                             + payments.dlq
```

- Rows are claimed with a worker ID and a **claim TTL**; if a worker dies, its
  claim lapses and another worker picks the row up. No external coordination.
- Backoff is exponential with **full jitter**, so events queued during one
  outage do not stampede the broker on recovery.
- Crashing after the Kafka write but before marking the row simply republishes
  the event later. That is at-least-once by design.
- **Retries never touch the ledger.** The publisher has no code path that writes
  to `balances` or `ledger_entries`.

---

## Kafka retry and replay flow

Delivery is **at-least-once**. Exactly-once *ledger effects* come from
PostgreSQL and idempotency, not from the broker.

```
  ┌──────────────┐  original event_id preserved  ┌──────────────┐
  │ outbox       │──────────────────────────────▶│ Kafka        │
  │ history      │   + replay metadata            │ same topic   │
  └──────────────┘                                └──────┬───────┘
                                                         ▼
                                            consumer dedupes on event_id
                                            → replayed event does no new work
```

Replay by transaction ID or time range, in deterministic `(created_at, id)`
order:

```bash
make replay TX=<transaction-id>
```

```bash
curl -X POST localhost:8080/v1/replay -H 'Idempotency-Key: r-1' \
  -d '{"transaction_id":"<id>","dry_run":true}'
```

Replay **reads** outbox history and **writes** to Kafka. It never opens a ledger
transaction, so it cannot create an entry or change a balance. Replayed events
carry `replay.is_replay=true` and a `replay_id` for observability, while keeping
their original `event_id` so consumers still recognise them as duplicates.

---

## Webhook retry flow

```
  Kafka event ──▶ webhook-worker
                     │
                     ├─ INSERT webhook_deliveries (event_id, endpoint) UNIQUE
                     │     duplicate event → no new row, no new work
                     │
                     └─ delivery loop (separate from consumption)
                            │
                 2xx ───────┼─────── non-2xx / timeout
                   │                        │
              delivered            attempts++, backoff, retry
                                            │
                              attempts >= max → dead_letter
```

Consumption and delivery are separated deliberately: the Kafka handler only
records *intent*, so a slow endpoint never blocks the consumer group, and every
pending delivery survives a restart because the state is in PostgreSQL.

The bundled receiver (`cmd/example-webhook`) records processed event IDs and
returns `200` for duplicates — exactly what a real consumer must do. It can be
told to fail on demand:

```bash
curl -X POST localhost:9090/fail?count=2   # fail the next two deliveries
curl -X POST localhost:9090/fail?always=true
curl localhost:9090/events                 # what it has processed
```

---

## Redis responsibilities and limitations

**Redis does**

- cache completed idempotency responses (fast path before PostgreSQL)
- enforce API rate limiting across replicas

**Redis never**

- holds authoritative balances, transactions or idempotency records
- participates in the payment transaction
- guards financial correctness with a lock

Every Redis call has a short timeout and degrades to a miss on any error. The
rate limiter **fails open**. Two tests hold this honest: one runs the whole
suite against a Redis that does not exist, and one calls `FLUSHDB` between a
request and its retry. Both must still show exactly one ledger transaction.

---

## Failure scenarios tested

| Scenario | Proven behaviour |
|---|---|
| Failure before `BEGIN` | No idempotency record; the key remains usable |
| Failure after locking accounts | Full rollback |
| Failure after entries written | Full rollback |
| Failure after balances updated | Full rollback |
| Failure after outbox written | Full rollback |
| Failure before `COMMIT` | No entries, no balance change, no outbox row |
| Panic mid-transaction | Rolled back; the pool stays usable |
| **Failure after commit, before response** | Money moved once; retry replays the original response |
| Concurrent duplicate requests (16×) | Exactly one ledger transaction |
| Concurrent overspending (25 × 1000 on 10000) | Exactly 10 succeed; balance floors at 0, never negative |
| Opposing transfers (80 simultaneous) | No deadlock; balances return to start |
| Concurrent reversals (8×) | Exactly one reversal created |
| Redis unavailable | Ledger fully correct; conflicts still detected |
| Redis flushed mid-flight | Retry still replays from PostgreSQL |
| Kafka unavailable | Payments still accepted; events queue in the outbox |
| Kafka restarted under a running consumer | The consumer reconnects with backoff instead of exiting |
| Outbox retry after Kafka failure | Bounded retries, then dead-letter; balances untouched |
| Restart with pending outbox events | Claim TTL lapses; a new worker republishes |
| Duplicate Kafka delivery (5×) | One delivery row, one outbound call |
| Event replay | Original event IDs; zero ledger change |
| Webhook temporary failure | Retried with backoff, then delivered |
| Webhook permanent failure | Dead-lettered at the attempt budget, then left alone |
| Restart with pending webhook deliveries | A new worker delivers them |
| Malformed / poison event | Discarded, does not block the consumer group |

Run them:

```bash
make test-integration
```

---

## Local setup

```bash
docker compose up --build
```

That starts PostgreSQL, Kafka (KRaft), Redis, the API on `:8080`, both workers,
and the example webhook receiver on `:9090`. Migrations run automatically.

Then, in another shell:

```bash
make seed     # create and fund demo accounts
make demo     # the full thirteen-step demonstration
```

### Everything else

```bash
make migrate            # apply migrations
make migrate-down       # revert them
make test               # unit tests, no infrastructure needed
make test-integration   # against real PostgreSQL, Kafka and Redis
make test-race          # the full suite under -race
make lint               # gofmt + go vet + golangci-lint
make replay TX=<id>     # replay a transaction's events
make reconcile          # recompute balances from entries
make smoke              # compose up + end-to-end check
make down               # tear everything down
```

Integration tests need the infrastructure running:

```bash
docker compose up -d postgres redis kafka
make test-integration
```

### Try it by hand

```bash
curl -X POST localhost:8080/v1/accounts -H 'Idempotency-Key: k1' \
  -d '{"id":"account-a","currency":"USD","kind":"user"}'
```

```bash
curl -X POST localhost:8080/v1/transfers -H 'Idempotency-Key: k2' \
  -d '{
    "source_account_id": "account-a",
    "destination_account_id": "account-b",
    "amount": 2500,
    "currency": "USD",
    "description": "Invoice payment",
    "external_reference": "invoice-4821"
  }'
```

---

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/v1/accounts` | Idempotency-Key required |
| `GET` | `/v1/accounts/{id}` | |
| `GET` | `/v1/accounts/{id}/balance` | |
| `GET` | `/v1/accounts/{id}/entries` | `?limit=&cursor=` keyset pagination |
| `POST` | `/v1/transfers` | Idempotency-Key required |
| `GET` | `/v1/transactions/{id}` | |
| `POST` | `/v1/transactions/{id}/reverse` | Idempotency-Key required |
| `POST` | `/v1/replay` | Idempotency-Key required |
| `GET` | `/v1/reconciliation` | 409 if the ledger disagrees with itself |
| `GET` | `/health/live`, `/health/ready` | |
| `GET` | `/metrics` | Prometheus |

Funding uses `POST /v1/transfers` with a system account as the source — there is
no privileged "mint" endpoint, because money must always come from somewhere.

**Error codes** are stable and drive the HTTP status:

| Code | Status |
|---|---|
| `invalid_request` | 400 |
| `currency_mismatch` | 400 |
| `unknown_account` | 404 |
| `transaction_not_found` | 404 |
| `insufficient_funds` | 422 |
| `idempotency_conflict` | 409 |
| `idempotency_in_progress` | 409 |
| `transaction_already_reversed` | 409 |
| `account_exists` | 409 |
| `rate_limited` | 429 |
| `dependency_unavailable` | 503 |
| `internal_error` | 500 |

---

## Observability

Structured JSON logs carry `request_id`, `correlation_id` and `transaction_id`,
propagated from HTTP through PostgreSQL, Kafka and webhook delivery.

Prometheus metrics include transfer success/failure, idempotency hit/conflict,
PostgreSQL retries by reason, Redis hit/miss/error, Kafka publish/retry, the
outbox backlog gauge, webhook success/retry/dead-letter, and reconciliation
mismatches.

---

## Documentation

- [docs/architecture.md](docs/architecture.md) — components and data flow
- [docs/invariants.md](docs/invariants.md) — the twelve invariants and their proofs
- [docs/failure-model.md](docs/failure-model.md) — what breaks and what happens
- [docs/event-delivery.md](docs/event-delivery.md) — outbox, Kafka, replay, webhooks
- [docs/threat-model.md](docs/threat-model.md) — trust boundaries and abuse cases

---

## Known limitations

This is an educational project. Honestly:

- **Not a payment processor.** No card networks, settlement, KYC/AML, or
  chargebacks. No PCI compliance is claimed or implied.
- **At-least-once messaging only.** No exactly-once distributed delivery. The
  strong guarantee is exactly-once *ledger effects*, via PostgreSQL and
  idempotency.
- **No authentication or authorisation.** Any caller can move any money. A real
  deployment needs authn/z, mTLS and per-tenant isolation.
- **Single-currency transactions.** No FX, no multi-currency transactions.
- **Single PostgreSQL primary.** No sharding or multi-region; availability is
  bounded by the database.
- **Reversals are complete only.** No partial reversals or refunds.
- **Idempotency records are kept forever.** Production needs a retention policy.
- **Failpoints are compiled in.** Inert unless `LEDGER_FAILPOINTS_ENABLED=true`,
  and the `/debug/failpoints` routes are not registered otherwise — but they
  must never be enabled in production.
- **Webhook signing is optional** and there is no endpoint management API.
- **Balances are a projection.** They are kept correct transactionally and
  audited by reconciliation, but they are derived data; entries are the truth.
