-- 0001_init.up.sql
-- Core double-entry ledger schema.
--
-- Design notes:
--   * Money is always a signed BIGINT in minor units (cents). Never float.
--   * Invariants are enforced by PostgreSQL constraints/triggers wherever
--     possible so that a buggy or malicious writer still cannot corrupt the
--     ledger. The Go layer re-checks for better error messages, not safety.

BEGIN;

CREATE TABLE accounts (
    id              TEXT        PRIMARY KEY,
    currency        CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    kind            TEXT        NOT NULL CHECK (kind IN ('user', 'system')),
    allow_overdraft BOOLEAN     NOT NULL DEFAULT FALSE,
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Target for the composite FK from ledger_entries: an entry's currency
    -- must equal the currency of the account it touches.
    CONSTRAINT accounts_id_currency_key UNIQUE (id, currency)
);

-- Balances live in their own table so that the hot row-level lock taken during
-- a transfer contends only with other balance writers, not with account reads.
CREATE TABLE balances (
    account_id      TEXT        PRIMARY KEY REFERENCES accounts (id) ON DELETE CASCADE,
    amount          BIGINT      NOT NULL DEFAULT 0,
    -- Denormalised from accounts so the non-negative rule can be a plain CHECK
    -- constraint instead of a trigger. Kept in sync by a trigger below.
    allow_overdraft BOOLEAN     NOT NULL DEFAULT FALSE,
    version         BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- INVARIANT 7: a non-overdraft account can never go negative, no matter
    -- how many transfers race. Enforced by the database, not by Go.
    CONSTRAINT balances_non_negative CHECK (allow_overdraft OR amount >= 0)
);

CREATE TABLE ledger_transactions (
    id                      UUID        PRIMARY KEY,
    kind                    TEXT        NOT NULL CHECK (kind IN ('transfer', 'funding', 'reversal')),
    currency                CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    description             TEXT        NOT NULL DEFAULT '',
    external_reference      TEXT,
    reverses_transaction_id UUID        REFERENCES ledger_transactions (id),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Target for the composite FK from ledger_entries: every entry in a
    -- transaction carries the transaction's single currency. This is how
    -- "one currency per transaction" is enforced declaratively.
    CONSTRAINT ledger_transactions_id_currency_key UNIQUE (id, currency),
    CONSTRAINT reversal_points_at_original CHECK (
        (kind = 'reversal') = (reverses_transaction_id IS NOT NULL)
    )
);

-- INVARIANT 9 (part 1): a transaction may be reversed at most once.
CREATE UNIQUE INDEX ledger_transactions_one_reversal
    ON ledger_transactions (reverses_transaction_id)
    WHERE reverses_transaction_id IS NOT NULL;

CREATE INDEX ledger_transactions_created_at_id
    ON ledger_transactions (created_at, id);

CREATE INDEX ledger_transactions_external_reference
    ON ledger_transactions (external_reference)
    WHERE external_reference IS NOT NULL;

CREATE TABLE ledger_entries (
    id             UUID        PRIMARY KEY,
    transaction_id UUID        NOT NULL REFERENCES ledger_transactions (id),
    account_id     TEXT        NOT NULL REFERENCES accounts (id),
    -- Positive = credit to the account, negative = debit from it.
    amount         BIGINT      NOT NULL CHECK (amount <> 0),
    currency       CHAR(3)     NOT NULL,
    seq            INT         NOT NULL CHECK (seq >= 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ledger_entries_tx_seq_key UNIQUE (transaction_id, seq),
    -- Entry currency == transaction currency (single-currency transactions).
    CONSTRAINT ledger_entries_tx_currency_fk
        FOREIGN KEY (transaction_id, currency)
        REFERENCES ledger_transactions (id, currency),
    -- Entry currency == account currency (no cross-currency postings).
    CONSTRAINT ledger_entries_account_currency_fk
        FOREIGN KEY (account_id, currency)
        REFERENCES accounts (id, currency)
);

CREATE INDEX ledger_entries_account_created ON ledger_entries (account_id, created_at DESC, id DESC);
CREATE INDEX ledger_entries_transaction ON ledger_entries (transaction_id);

-- INVARIANT 1: every committed transaction sums to exactly zero and has at
-- least two entries. Checked as a DEFERRED constraint trigger so the check
-- runs once at COMMIT, after all entries of the transaction are inserted.
CREATE FUNCTION assert_transaction_balanced() RETURNS TRIGGER AS $$
DECLARE
    entry_sum   BIGINT;
    entry_count INT;
BEGIN
    SELECT COALESCE(SUM(amount), 0), COUNT(*)
      INTO entry_sum, entry_count
      FROM ledger_entries
     WHERE transaction_id = NEW.transaction_id;

    IF entry_count < 2 THEN
        RAISE EXCEPTION 'ledger transaction % has % entries, need at least 2',
            NEW.transaction_id, entry_count
            USING ERRCODE = 'check_violation';
    END IF;

    IF entry_sum <> 0 THEN
        RAISE EXCEPTION 'ledger transaction % does not balance: sum=%',
            NEW.transaction_id, entry_sum
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER ledger_entries_balanced
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_transaction_balanced();

-- INVARIANT 2: the ledger is append-only. Entries and transactions are
-- immutable once written; corrections happen only via reversal transactions.
CREATE FUNCTION reject_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'relation % is append-only: % is not permitted',
        TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entries_immutable
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

CREATE TRIGGER ledger_transactions_immutable
    BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

-- Keep balances.allow_overdraft in step with the owning account.
CREATE FUNCTION sync_balance_overdraft() RETURNS TRIGGER AS $$
BEGIN
    UPDATE balances
       SET allow_overdraft = NEW.allow_overdraft
     WHERE account_id = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER accounts_sync_overdraft
    AFTER UPDATE OF allow_overdraft ON accounts
    FOR EACH ROW EXECUTE FUNCTION sync_balance_overdraft();

CREATE TABLE reversals (
    id                      UUID        PRIMARY KEY,
    original_transaction_id UUID        NOT NULL UNIQUE REFERENCES ledger_transactions (id),
    reversal_transaction_id UUID        NOT NULL UNIQUE REFERENCES ledger_transactions (id),
    reason                  TEXT        NOT NULL DEFAULT '',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT reversal_is_not_self CHECK (original_transaction_id <> reversal_transaction_id)
);

-- Durable idempotency. This is the source of truth; Redis is only a cache.
CREATE TABLE idempotency_records (
    scope           TEXT        NOT NULL,
    key             TEXT        NOT NULL,
    request_hash    TEXT        NOT NULL,
    state           TEXT        NOT NULL CHECK (state IN ('in_progress', 'completed')),
    response_status INT,
    -- BYTEA, not JSONB, on purpose: jsonb normalises key order and whitespace,
    -- so a replayed response would be semantically equal but not byte-equal to
    -- the original. Storing the exact bytes makes an idempotent retry return
    -- precisely what the first call returned.
    response_body   BYTEA,
    transaction_id  UUID        REFERENCES ledger_transactions (id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (scope, key),
    CONSTRAINT completed_has_response CHECK (
        state <> 'completed' OR (response_status IS NOT NULL AND response_body IS NOT NULL)
    )
);

CREATE INDEX idempotency_records_transaction ON idempotency_records (transaction_id)
    WHERE transaction_id IS NOT NULL;

-- Transactional outbox: written in the same PostgreSQL transaction as the
-- ledger changes, drained to Kafka by a separate worker.
CREATE TABLE outbox_events (
    id              UUID        PRIMARY KEY,
    topic           TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    partition_key   TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    headers         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'publishing', 'published', 'dead_letter')),
    attempts        INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error      TEXT,
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT published_has_timestamp CHECK ((status = 'published') = (published_at IS NOT NULL))
);

-- Drives the publisher's claim query; only unfinished rows are indexed.
CREATE INDEX outbox_events_claimable
    ON outbox_events (next_attempt_at, created_at)
    WHERE status IN ('pending', 'publishing');

CREATE INDEX outbox_events_aggregate ON outbox_events (aggregate_id);
CREATE INDEX outbox_events_created_at ON outbox_events (created_at, id);

-- Webhook fan-out state, owned by the webhook worker.
CREATE TABLE webhook_deliveries (
    id              UUID        PRIMARY KEY,
    event_id        UUID        NOT NULL,
    endpoint        TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    transaction_id  TEXT        NOT NULL DEFAULT '',
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'delivered', 'dead_letter')),
    attempts        INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error      TEXT,
    last_status_code INT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- INVARIANT 11: duplicate Kafka deliveries of the same event cannot create
    -- duplicate webhook state. At-least-once in, exactly-one row out.
    CONSTRAINT webhook_deliveries_event_endpoint_key UNIQUE (event_id, endpoint)
);

CREATE INDEX webhook_deliveries_claimable
    ON webhook_deliveries (next_attempt_at, created_at)
    WHERE status = 'pending';

COMMIT;
