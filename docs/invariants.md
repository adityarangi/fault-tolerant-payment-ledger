# Invariants

Twelve properties this ledger holds, how each is enforced, and the test that
would catch a regression. "Enforced by PostgreSQL" means a buggy or malicious
writer still cannot violate it.

---

## 1. Every committed ledger transaction sums to zero

**Enforced by:** a `DEFERRABLE INITIALLY DEFERRED` constraint trigger
(`ledger_entries_balanced`) that runs at `COMMIT`, after all entries are
inserted. It also requires at least two entries.

Deferral matters: entries are inserted one at a time, so the transaction is
transiently unbalanced. Checking at `COMMIT` is the only correct moment.

**Tests:** `TestBalancedTransfer`, `TestUnbalancedTransactionRejected` (raw SQL
insert of a mismatched pair — the `COMMIT` fails), `TestSingleEntryTransactionRejected`.

---

## 2. Ledger entries are immutable

**Enforced by:** `BEFORE UPDATE OR DELETE` triggers on `ledger_entries` and
`ledger_transactions` that raise an exception unconditionally. There is no
application path that attempts a mutation; the trigger exists to make the
guarantee independent of the application.

Corrections happen only through reversal transactions.

**Test:** `TestLedgerEntriesAreImmutable` issues direct `UPDATE` and `DELETE`
statements against the database and requires all of them to fail.

---

## 3. Account balances equal the sum of committed entries

**Enforced by:** balance updates happening in the same transaction as the
entries that justify them, under the same row locks.

Balances are a *projection* kept for fast reads; the entries are the truth. The
reconciliation job recomputes balances from entries and reports any divergence,
incrementing `ledger_reconciliation_mismatch_total`.

**Tests:** `TestBalanceReconciliation` (mixed transfers and a reversal, then a
full recompute), plus every other integration test — the harness calls
`requireReconciled` at the end.

---

## 4. A successful idempotency key creates at most one ledger transaction

**Enforced by:** the `(scope, key)` primary key on `idempotency_records`,
written **inside the payment transaction**. A second attempt to claim the key
finds the existing row and replays instead of executing.

A transaction-scoped advisory lock serialises concurrent same-key requests so
they queue cleanly rather than racing to the unique-violation. The primary key
remains the actual guarantee.

**Tests:** `TestDuplicateIdempotentRequest`, `TestConcurrentDuplicateRequests`
(16 goroutines released simultaneously; the test counts rows in
`ledger_transactions` and requires exactly one).

---

## 5. Failed PostgreSQL transactions leave no partial state

**Enforced by:** doing all of it in one transaction — entries, balances,
idempotency record and outbox event — and rolling back on any error, including
a panic (the transaction helper rolls back in a `defer` before re-panicking).

**Test:** `TestFailureBeforeCommit` walks five failpoints (`after_accounts_locked`,
`after_entries_written`, `after_balances_updated`, `after_outbox_written`,
`before_commit`) and after each one asserts unchanged counts of transactions,
entries and outbox rows, and an unchanged balance. `TestPanicDuringTransactionRollsBack`
does the same for a panic and then proves the pool still works.

---

## 6. Lost HTTP responses can be retried without duplicate payment effects

**Enforced by:** the durable idempotency record committing atomically with the
payment. If the client never sees the response, the record is nonetheless
committed, so the retry replays it.

The stored response is raw bytes (`BYTEA`, not `JSONB`) so the replay is
byte-identical rather than merely equivalent.

**Test:** `TestFailureAfterCommitBeforeResponse` uses the `after_commit`
failpoint: the client gets a 500, the money has moved, and the retry returns
201 with the original body and `X-Idempotent-Replay: true` while the
transaction count stays flat.

---

## 7. Concurrent transfers cannot overspend an account

**Enforced by:** `CHECK (allow_overdraft OR amount >= 0)` on `balances`. The Go
funds check produces a clean error message; the constraint is what makes it
true. A `CHECK` violation aborts the whole transaction, so a losing racer
commits nothing.

Only `kind = 'system'` accounts may enable overdraft — that is where money is
issued from, and their negative balance is exactly the money in circulation.

**Test:** `TestConcurrentOverspending` fires 25 concurrent transfers of 1000
against a balance of 10000 and requires exactly 10 successes, 15
`insufficient_funds`, and a final balance of 0.

---

## 8. Opposing transfers do not deadlock indefinitely

**Enforced by:** deterministic lock ordering — balance rows are always locked in
ascending account-ID order, so A→B and B→A request the same locks in the same
sequence and queue instead of cycling. Balance updates are applied in that same
sorted order.

Defence in depth: `lock_timeout` and `statement_timeout` bound any wait, and
deadlock (`40P01`), serialization (`40001`) and lock-timeout errors are retried
with exponential backoff and full jitter.

**Test:** `TestOpposingTransfersDoNotDeadlock` runs 40 transfers in each
direction simultaneously with a 60-second watchdog, then checks both balances
returned to their starting values.

---

## 9. Reversals preserve history and restore the financial effect

**Enforced by:** a reversal being a *new* transaction with mirrored entries
(each `amount` negated, so it also sums to zero), linked by
`reverses_transaction_id`, plus a row in `reversals`. The original transaction
is never touched — it cannot be, per invariant 2.

At most one reversal per transaction, guaranteed by a partial unique index on
`reverses_transaction_id` and a `UNIQUE` constraint on
`reversals.original_transaction_id`.

**Tests:** `TestReversal` (balances restored, original intact and linked to its
reversal), `TestDuplicateReversal` (409 `transaction_already_reversed`),
`TestReversalIsIdempotent`, `TestConcurrentReversalsCreateOne` (8 simultaneous
attempts → exactly one).

---

## 10. Kafka retries and event replay never modify ledger balances

**Enforced by:** structural separation. The publisher and the replayer only ever
read outbox history and write to Kafka plus the outbox's own bookkeeping
columns. Neither has a code path that writes `balances`, `ledger_entries` or
`ledger_transactions`.

**Tests:** `TestOutboxRetryAfterKafkaFailure` records balances and entry counts
before a full retry-to-dead-letter cycle against a dead broker and requires them
unchanged. `TestEventReplay` does the same around a real replay and additionally
checks the replayed event kept its original event ID.

---

## 11. Duplicate Kafka events do not create duplicate webhook state

**Enforced by:** `UNIQUE (event_id, endpoint)` on `webhook_deliveries`, with the
insert written as `ON CONFLICT DO NOTHING`. At-least-once in, exactly one
delivery row out. Because replayed events keep their original event ID, a replay
is recognised as a duplicate too.

**Tests:** `TestDuplicateKafkaDeliveryIsIdempotent` (the same event handled five
times → one row, one outbound call),
`TestReplayedEventDoesNotDuplicateWebhookState`.

---

## 12. Redis failure does not compromise ledger correctness

**Enforced by:** Redis being outside the payment transaction entirely. The cache
is read before PostgreSQL and written after commit; every call has a short
timeout and treats any error as a miss. The rate limiter fails open.

**Tests:** `TestRedisUnavailable` points the client at a closed port and runs the
full idempotency behaviour — including conflict detection — through PostgreSQL.
`TestRedisFlushedMidFlight` calls `FLUSHDB` between a request and its retry and
requires the retry to still replay the original response with no extra
transaction.

---

## How to check them yourself

```bash
docker compose up -d postgres redis kafka
make test-integration
make test-race
```

Reconciliation can be run against a live system at any time:

```bash
make reconcile
```

It returns HTTP 409 and a list of offending accounts and transactions if the
ledger ever disagrees with itself.
