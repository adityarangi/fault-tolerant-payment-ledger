# Event delivery

## Guarantee, stated precisely

- Kafka delivery is **at-least-once**.
- Ledger effects are **exactly-once**, enforced by PostgreSQL transactions and
  idempotency.
- Consumers must be **idempotent**, keyed on `event_id`.

This project does not implement exactly-once distributed messaging, and does not
claim to. The interesting guarantee is not "the message arrives once" — it is
"no matter how many times the message arrives, the books are right".

## Envelope

```json
{
  "event_id": "d2b1…",
  "event_type": "payment.created",
  "schema_version": 1,
  "transaction_id": "7f3c…",
  "occurred_at": "2025-03-01T12:00:00Z",
  "correlation_id": "req-abc",
  "replay": null,
  "data": {
    "transaction_id": "7f3c…",
    "kind": "transfer",
    "currency": "USD",
    "amount": 2500,
    "source_account_id": "account-a",
    "destination_account_id": "account-b",
    "description": "Invoice payment",
    "external_reference": "invoice-4821",
    "created_at": "2025-03-01T12:00:00Z",
    "entries": [
      {"entry_id": "…", "account_id": "account-a", "amount": -2500, "currency": "USD", "seq": 0},
      {"entry_id": "…", "account_id": "account-b", "amount":  2500, "currency": "USD", "seq": 1}
    ]
  }
}
```

`event_id` is the deduplication key. It is assigned once, when the outbox row is
written inside the payment transaction, and never changes — including across
replays. `transaction_id` groups events for one payment and is the Kafka
partition key, so events about a single transaction stay ordered.

Kafka headers carry `event_id`, `transaction_id`, `event_type`,
`correlation_id` and `schema_version`, so a consumer can deduplicate without
parsing the body.

## Topics

| Topic | Contents |
|---|---|
| `payments.created` | Transfers and funding |
| `payments.reversed` | Reversals |
| `payments.failed` | Failure notifications |
| `payments.dlq` | Events that exhausted their publish budget |

## Outbox lifecycle

```
pending ──claim──▶ publishing ──success──▶ published
   ▲                    │
   └──failure, backoff──┘
                        └──attempts >= max──▶ dead_letter (+ payments.dlq)
```

- **Claiming** uses `SELECT … FOR UPDATE SKIP LOCKED` inside an `UPDATE`, so
  multiple replicas take disjoint batches without blocking each other.
- **Claim TTL** (`LEDGER_OUTBOX_CLAIM_TTL`) makes a dead worker's rows claimable
  again. This is the entire crash-recovery mechanism — no leader election, no
  external lock.
- **Backoff** is exponential with full jitter, capped at
  `LEDGER_OUTBOX_MAX_BACKOFF`. Jitter prevents a thundering herd when a broker
  returns after an outage.
- **Dead-lettering** stops unbounded retries. The row stays in PostgreSQL with
  its `last_error`, so nothing is lost and `cmd/replay` can republish it later.

Publishing uses `acks=all` with client-side retries disabled — the outbox owns
retries, and letting the client retry too would double-count attempts.

## Replay

```bash
make replay TX=<transaction-id>
```

```bash
go run ./cmd/replay -from 2025-03-01T00:00:00Z -to 2025-03-02T00:00:00Z
go run ./cmd/replay -transaction <id> -dry-run
```

```bash
curl -X POST localhost:8080/v1/replay -H 'Idempotency-Key: r-1' \
  -d '{"transaction_id":"7f3c…"}'
```

Properties:

- **Deterministic order.** Always `ORDER BY created_at, id`.
- **Original event IDs preserved**, so idempotent consumers recognise replays as
  duplicates and do no new work.
- **Clearly marked.** `replay.is_replay`, `replay_id`, `replayed_at`,
  `replayed_by`, plus `replay: true` in the Kafka headers.
- **Read-only with respect to the ledger.** Replay never opens a ledger
  transaction. It cannot create an entry or change a balance.
- **Bounded.** A replay must specify a transaction ID and/or a time range;
  an unbounded request is rejected.

Replaying is the recovery tool for a downstream consumer that lost data, and for
outbox rows that were dead-lettered during a long broker outage.

## Webhook delivery

The Kafka handler and the HTTP sender are deliberately separate:

```
consume ──▶ INSERT webhook_deliveries (pending)   ──▶ commit offset
                            │
poll ───────────────────────┴──▶ POST endpoint
                                   2xx → delivered
                                   else → attempts++, backoff, retry
                                          attempts >= max → dead_letter
```

Recording intent and then delivering means a slow endpoint cannot stall the
consumer group, and every pending delivery survives a restart because it lives
in PostgreSQL.

`UNIQUE (event_id, endpoint)` is what makes at-least-once safe: a redelivered
or replayed event finds the row already present and creates nothing.

Outbound requests carry:

| Header | Meaning |
|---|---|
| `X-Ledger-Event-Id` | Deduplication key |
| `X-Ledger-Event-Type` | e.g. `payment.created` |
| `X-Ledger-Transaction-Id` | The payment |
| `X-Ledger-Delivery-Attempt` | 1-based attempt number |
| `X-Ledger-Signature` | `sha256=…` HMAC, when a signing secret is configured |

## What a consumer must do

1. Read `event_id`.
2. If it has been processed, return `200` and do nothing else. **Do not** return
   an error for a duplicate — that just causes retries.
3. Otherwise process the event and record the ID atomically with the work.
4. Return `2xx` only after the work is durable.

`cmd/example-webhook` is a working reference: it tracks processed IDs, returns
`200 duplicate_ignored` for repeats, and can be told to fail on demand
(`POST /fail?count=2`) so retry and dead-letter behaviour can be exercised.
