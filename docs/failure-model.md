# Failure model

What can go wrong, what the system does about it, and how that behaviour is
proven.

## Failpoints

Failure injection points, inert unless `LEDGER_FAILPOINTS_ENABLED=true`:

| Failpoint | Position |
|---|---|
| `before_tx_begin` | Before opening the PostgreSQL transaction |
| `after_accounts_locked` | After balance rows are locked |
| `after_entries_written` | After ledger entries are inserted |
| `after_balances_updated` | After balances are updated |
| `after_outbox_written` | After the outbox event is written |
| `before_commit` | Immediately before `COMMIT` |
| `after_commit` | Committed, before the HTTP response |
| `before_kafka_publish` | Before producing to Kafka |
| `after_kafka_publish` | Published, before marking the outbox row |
| `before_webhook_persist` | Before webhook delivery state is persisted |
| `before_webhook_send` | Before the outbound webhook call |

Actions: `error`, `panic`, `sleep:<duration>`, each optionally bounded by a
count (`error:2` fires twice, then stops).

Arm them three ways:

```bash
# process-wide
LEDGER_FAILPOINTS="before_commit=error,after_commit=error:1"
```

```bash
# per request — this is what the integration tests use
curl -X POST localhost:8080/v1/transfers \
  -H 'Idempotency-Key: k1' -H 'X-Failpoint: after_commit=error' -d '{...}'
```

```bash
# at runtime
curl -X POST localhost:8080/debug/failpoints -d '{"name":"before_commit","action":"error:1"}'
curl -X DELETE localhost:8080/debug/failpoints
```

The `/debug/failpoints` routes are not registered at all unless failpoints are
enabled.

---

## Failure catalogue

### The client disappears mid-request

Nothing special happens. Either the transaction committed (the client can
retry and get the original response) or it did not (the retry executes). Both
are safe; the client cannot tell which, and does not need to.

### The response is lost after commit

**The important one.** Money moved; the client saw an error or a timeout.

The idempotency record committed atomically with the payment, so a retry with
the same key returns the original response byte-for-byte, with
`X-Idempotent-Replay: true`. No second transaction.

*Proven by `TestFailureAfterCommitBeforeResponse`.*

### The API process dies mid-transaction

PostgreSQL rolls back the transaction when the connection drops. No entries, no
balance change, no outbox row, no idempotency record — the key stays usable.

*Proven by `TestFailureBeforeCommit` and `TestPanicDuringTransactionRollsBack`.*

### Two transfers race on the same account

Balance rows are locked in ascending account-ID order. The loser waits, then
sees the winner's committed balance. If it can no longer afford the transfer it
gets `insufficient_funds`; the `CHECK` constraint is the backstop if the Go
check were ever wrong.

*Proven by `TestConcurrentOverspending`.*

### Two transfers race in opposite directions

Same sorted lock order, so they queue rather than cycle. `lock_timeout` bounds
the wait, and deadlock/serialization errors are retried with jittered backoff.

*Proven by `TestOpposingTransfersDoNotDeadlock`.*

### PostgreSQL is unavailable

The API returns `503 dependency_unavailable` and `/health/ready` reports not
ready. Nothing is accepted, which is the correct behaviour — there is no
degraded mode for the source of truth. Workers back off and resume.

### Redis is unavailable or flushed

Cache reads return "miss" and fall through to PostgreSQL; cache writes are
dropped; the rate limiter fails open. Latency rises, correctness does not
change.

*Proven by `TestRedisUnavailable` and `TestRedisFlushedMidFlight`.*

### Kafka is unavailable

Payments continue to be accepted — the event is already durable in the outbox.
The publisher retries with exponential backoff plus full jitter. The
`ledger_outbox_backlog` gauge rises; that is the alert signal.

Consumers must survive the outage too. A failed fetch is retried with bounded,
jittered backoff rather than treated as fatal — an earlier version of this code
exited the consumer on the first fetch error, which permanently stranded the
webhook worker after a transient Kafka restart. The consumer now returns only
on shutdown or a closed reader.

*Proven by `TestKafkaUnavailableDoesNotBlockPayments` and
`TestConsumerSurvivesBrokerOutage`.*

### Kafka stays unavailable past the attempt budget

The row moves to `dead_letter` after `LEDGER_OUTBOX_MAX_ATTEMPTS` and a best-effort
copy is written to `payments.dlq`. The event is not lost: the row remains in
PostgreSQL with its error, and `cmd/replay` can republish it once the broker is
healthy. Balances are untouched throughout.

*Proven by `TestOutboxRetryAfterKafkaFailure`.*

### The outbox worker dies mid-publish

Two sub-cases:

- **Before publishing.** The claim lapses after `LEDGER_OUTBOX_CLAIM_TTL` and
  another worker picks the row up.
- **After publishing, before marking the row.** The event is published *again*
  later. This is at-least-once delivery working as designed; consumers
  deduplicate on `event_id`.

*Proven by `TestOutboxRecoversAfterRestart`.*

### Two outbox workers run at once

`FOR UPDATE SKIP LOCKED` gives each a disjoint set of rows. Neither blocks the
other.

*Proven by `TestOutboxClaimIsExclusive`.*

### Kafka delivers an event twice

The webhook worker's insert hits `UNIQUE (event_id, endpoint)` and does nothing.
One delivery row, one outbound call.

*Proven by `TestDuplicateKafkaDeliveryIsIdempotent`.*

### A webhook endpoint is slow or failing

Consumption and delivery are separate loops, so a bad endpoint never stalls the
consumer group. Failures increment `attempts`, record `last_error` and schedule
a jittered retry. At the attempt budget the delivery is dead-lettered and left
alone.

*Proven by `TestWebhookTemporaryFailureAndRetry` and `TestWebhookDeadLetter`.*

### The webhook worker restarts with deliveries pending

Delivery state is in PostgreSQL, not memory. A new process picks up every
`pending` row whose `next_attempt_at` has passed.

*Proven by `TestWebhookRecoversPendingDeliveriesAfterRestart`.*

### A poison message arrives

A message that cannot be parsed, or that carries no `event_id`, is logged and
discarded — its offset is committed. Retrying it forever would block the
partition, and it will never become valid.

*Proven by `TestWebhookMalformedEventIsDiscarded`.*

### Someone replays history by accident

Replay only reads outbox history and writes to Kafka. It cannot create entries
or change balances. Replayed events keep their original `event_id`, so
idempotent consumers do no new work; `replay.is_replay` and `replay_id` make it
visible in logs and metrics.

*Proven by `TestEventReplay`.*

### Balances drift from entries

They should not — they are written in the same transaction — but the claim is
worth auditing rather than trusting. `GET /v1/reconciliation` recomputes every
balance from `ledger_entries` and re-checks that every transaction sums to
zero, returning HTTP 409 and incrementing
`ledger_reconciliation_mismatch_total` on any disagreement.

---

## Not handled

Stated plainly, because a failure model is only useful if it says where it ends:

- **Byte-level database corruption.** Out of scope; relies on PostgreSQL and
  its backups.
- **Loss of the PostgreSQL primary.** No automated failover or multi-region
  replication here.
- **Malicious operators.** Anyone with database write access can bypass the
  application, though the immutability and zero-sum triggers still resist
  casual tampering.
- **Clock skew across services.** Timestamps are informational; ordering that
  matters uses `(created_at, id)` from a single database clock.
- **Poison-message quarantine.** Malformed events are dropped and logged rather
  than routed to a quarantine topic for inspection.
