-- 0001_init.down.sql
BEGIN;

DROP TABLE IF EXISTS webhook_deliveries;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS idempotency_records;
DROP TABLE IF EXISTS reversals;
DROP TRIGGER IF EXISTS accounts_sync_overdraft ON accounts;
DROP TRIGGER IF EXISTS ledger_transactions_immutable ON ledger_transactions;
DROP TRIGGER IF EXISTS ledger_entries_immutable ON ledger_entries;
DROP TRIGGER IF EXISTS ledger_entries_balanced ON ledger_entries;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS ledger_transactions;
DROP TABLE IF EXISTS balances;
DROP TABLE IF EXISTS accounts;
DROP FUNCTION IF EXISTS sync_balance_overdraft();
DROP FUNCTION IF EXISTS reject_mutation();
DROP FUNCTION IF EXISTS assert_transaction_balanced();

COMMIT;
