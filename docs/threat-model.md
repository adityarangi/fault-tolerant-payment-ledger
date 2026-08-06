# Threat model

Scope: the ledger service as built here. This is an educational project with
**no authentication or authorisation** — that is the largest gap and is stated
first deliberately.

## Trust boundaries

```
   untrusted            semi-trusted              trusted
   ─────────            ────────────              ───────
   HTTP clients  ──▶    API process        ──▶    PostgreSQL
                        workers            ──▶    (source of truth)
                             │
                             ├──▶ Kafka   (internal network)
                             ├──▶ Redis   (internal network)
                             └──▶ webhook endpoints (untrusted, outbound)
```

Everything a client sends is untrusted input. Kafka and Redis are treated as
internal but not as sources of truth. Webhook endpoints are untrusted
destinations.

## What is defended

### Ledger tampering

Entries and transactions are immutable at the database level; the zero-sum rule
is a deferred constraint trigger; balances cannot go negative for non-overdraft
accounts. An application bug cannot corrupt the books, and reconciliation
detects any divergence after the fact.

### Duplicate or replayed payments

Every mutation requires an `Idempotency-Key`, backed by a durable PostgreSQL
record written in the payment transaction. Replaying a captured request with the
same key returns the original response and moves no money. Replaying it with a
*modified* body is rejected with `idempotency_conflict` — the request hash
covers the full canonical payload plus the endpoint scope.

### Cross-currency confusion

Composite foreign keys pin each entry's currency to both its transaction and its
account. Mixing currencies is a foreign-key violation, not a rounding surprise.

### Resource exhaustion

Request bodies are capped at 1 MiB; `limit` on entry listing is bounded;
replay requires a selector and caps at 10,000 events; the Redis-backed rate
limiter throttles per API key or client IP. Connection pools, `lock_timeout` and
`statement_timeout` bound database resource use.

### Injection

All SQL uses parameterised queries via pgx. Account IDs are validated against a
strict pattern; currencies must match `^[A-Z]{3}$`, enforced both in Go and by a
`CHECK` constraint.

### Information disclosure in errors

Client-facing errors carry a stable code and a safe message. Underlying driver
errors are attached as a cause for logging only and never serialised into the
response body.

### Webhook authenticity

When `LEDGER_WEBHOOK_SIGNING_SECRET` is set, deliveries carry an
`X-Ledger-Signature: sha256=…` HMAC over the exact payload bytes, letting
receivers verify origin and integrity.

### Accidental failure injection

Failpoints are inert unless `LEDGER_FAILPOINTS_ENABLED=true`. The
`X-Failpoint` header is ignored otherwise, and the `/debug/failpoints` routes
are not registered at all. Startup logs a warning when injection is enabled.

## What is *not* defended

These are real gaps, not oversights to gloss over:

### No authentication or authorisation

**Any caller can create accounts and move any money.** There is no identity, no
tenancy, no per-account permission check. A real deployment needs
authentication, per-tenant isolation, and authorisation on every account
reference. Do not expose this service to an untrusted network.

### No transport security

Plain HTTP, and plaintext connections to PostgreSQL, Kafka and Redis in the
compose setup. Production needs TLS everywhere and mTLS between services.

### No secret management

Configuration comes from environment variables; `.env.example` contains
placeholders only. There is no integration with a secrets manager and no
rotation story.

### No audit trail of *who*

The ledger records what happened, immutably, with request and correlation IDs —
but there is no authenticated actor to attribute it to, because there is no
authentication.

### Webhook SSRF

Endpoints come from configuration, not from user input, which limits the risk.
But there is no allowlist, no IP-range restriction, and no protection against an
operator configuring an internal address.

### Replay as a denial-of-service vector

`POST /v1/replay` can republish up to 10,000 events. It is rate-limited and
requires a selector, but it is not privileged. It should require elevated
authorisation once authentication exists.

### No PII handling

`description`, `external_reference` and account metadata are stored as supplied
and forwarded to webhooks. There is no field-level encryption, redaction or
retention policy. Do not put sensitive personal data in them.

### Unbounded idempotency retention

Records are kept forever. This is a storage and privacy concern; production
needs a retention policy that outlives the longest plausible client retry.

### No PCI scope

No card data is handled and **no PCI compliance is claimed or implied**. Nothing
here has been assessed against PCI DSS.

## If this were going to production

In rough priority order:

1. Authentication, authorisation and per-tenant isolation.
2. TLS/mTLS on every hop; secrets from a managed store.
3. `LEDGER_FAILPOINTS_ENABLED=false`, enforced by deployment policy.
4. Retention policies for idempotency records and webhook history.
5. Reconciliation on a schedule, alerting on
   `ledger_reconciliation_mismatch_total` and `ledger_outbox_backlog`.
6. An egress allowlist for webhook endpoints.
7. Database failover, backups and restore drills.
