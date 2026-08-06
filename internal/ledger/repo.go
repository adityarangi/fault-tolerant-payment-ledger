package ledger

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adityarangi/payment-ledger/internal/apperr"
	"github.com/adityarangi/payment-ledger/internal/idempotency"
	"github.com/adityarangi/payment-ledger/internal/storage"
)

// lockedAccount is the account state read under a row lock.
type lockedAccount struct {
	ID             string
	Currency       string
	Kind           string
	AllowOverdraft bool
	Balance        int64
	Version        int64
}

// lockBalances locks the balance rows for the given accounts in ascending
// account-ID order.
//
// The deterministic order is the whole point: two transfers in opposite
// directions between the same pair of accounts always request the same locks
// in the same sequence, so they queue instead of forming a cycle
// (INVARIANT 8). lock_timeout on the connection bounds the wait even if some
// other session takes locks out of order.
func lockBalances(ctx context.Context, tx pgx.Tx, accountIDs []string) (map[string]*lockedAccount, error) {
	ordered := make([]string, len(accountIDs))
	copy(ordered, accountIDs)
	sort.Strings(ordered)

	out := make(map[string]*lockedAccount, len(ordered))
	for _, id := range ordered {
		if _, seen := out[id]; seen {
			continue
		}
		var acct lockedAccount
		err := tx.QueryRow(ctx, `
            SELECT a.id, a.currency, a.kind, b.allow_overdraft, b.amount, b.version
              FROM balances b
              JOIN accounts a ON a.id = b.account_id
             WHERE b.account_id = $1
               FOR NO KEY UPDATE OF b`, id).
			Scan(&acct.ID, &acct.Currency, &acct.Kind, &acct.AllowOverdraft, &acct.Balance, &acct.Version)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, apperr.UnknownAccount(id)
		}
		if err != nil {
			return nil, fmt.Errorf("ledger: lock balance %q: %w", id, err)
		}
		out[id] = &acct
	}
	return out, nil
}

// applyBalanceDelta moves an account's balance by delta.
//
// The non-negative rule is a CHECK constraint on balances, so even if the
// in-Go funds check were wrong or raced, PostgreSQL rejects the write. That
// rejection is translated back into insufficient_funds here.
func applyBalanceDelta(ctx context.Context, tx pgx.Tx, accountID string, delta int64) (int64, error) {
	var amount int64
	err := tx.QueryRow(ctx, `
        UPDATE balances
           SET amount = amount + $2, version = version + 1, updated_at = now()
         WHERE account_id = $1
     RETURNING amount`, accountID, delta).Scan(&amount)
	if storage.IsCheckViolation(err, "balances_non_negative") {
		return 0, apperr.InsufficientFunds(accountID)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, apperr.UnknownAccount(accountID)
	}
	if err != nil {
		return 0, fmt.Errorf("ledger: update balance %q: %w", accountID, err)
	}
	return amount, nil
}

// insertTransaction writes the ledger transaction header.
func insertTransaction(ctx context.Context, tx pgx.Tx, t *Transaction) error {
	_, err := tx.Exec(ctx, `
        INSERT INTO ledger_transactions
            (id, kind, currency, description, external_reference, reverses_transaction_id)
        VALUES ($1, $2, $3, $4, $5, $6)`,
		t.ID, t.Kind, t.Currency, t.Description, t.ExternalReference, t.ReversesTransactionID)
	if storage.IsUniqueViolation(err, "ledger_transactions_one_reversal") {
		return apperr.New(apperr.CodeTransactionAlreadyReversed,
			"transaction %q has already been reversed", derefString(t.ReversesTransactionID)).
			WithDetail("transaction_id", derefString(t.ReversesTransactionID))
	}
	if err != nil {
		return fmt.Errorf("ledger: insert transaction: %w", err)
	}
	return nil
}

// insertEntries writes the postings. The deferred balance trigger validates
// the zero-sum rule at COMMIT.
func insertEntries(ctx context.Context, tx pgx.Tx, entries []Entry) error {
	for _, e := range entries {
		_, err := tx.Exec(ctx, `
            INSERT INTO ledger_entries (id, transaction_id, account_id, amount, currency, seq)
            VALUES ($1, $2, $3, $4, $5, $6)`,
			e.ID, e.TransactionID, e.AccountID, e.Amount, e.Currency, e.Seq)
		if storage.IsForeignKeyViolation(err) {
			// The composite FKs encode "entry currency must equal both the
			// transaction currency and the account currency".
			return apperr.CurrencyMismatch("entry for account %q does not match the transaction currency %s",
				e.AccountID, e.Currency)
		}
		if err != nil {
			return fmt.Errorf("ledger: insert entry: %w", err)
		}
	}
	return nil
}

func insertReversalRecord(ctx context.Context, tx pgx.Tx, id, originalID, reversalID, reason string) error {
	_, err := tx.Exec(ctx, `
        INSERT INTO reversals (id, original_transaction_id, reversal_transaction_id, reason)
        VALUES ($1, $2, $3, $4)`, id, originalID, reversalID, reason)
	if storage.IsUniqueViolation(err, "") {
		return apperr.New(apperr.CodeTransactionAlreadyReversed,
			"transaction %q has already been reversed", originalID).
			WithDetail("transaction_id", originalID)
	}
	if err != nil {
		return fmt.Errorf("ledger: insert reversal: %w", err)
	}
	return nil
}

// CreateAccount inserts an account and its zero balance row, together with the
// durable idempotency record, in one transaction.
func (s *Service) CreateAccount(ctx context.Context, cmd CreateAccountCommand) (*Result, error) {
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	metadata := cmd.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, apperr.InvalidRequest("metadata must be JSON-serialisable")
	}

	var result *Result
	err = s.db.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := idempotency.LockKey(ctx, tx, cmd.IdempotencyScope, cmd.IdempotencyKey); err != nil {
			return err
		}
		existing, err := idempotency.Claim(ctx, tx, cmd.IdempotencyScope, cmd.IdempotencyKey, cmd.RequestHash)
		if err != nil {
			return err
		}
		if existing != nil {
			replayed, err := replayResult(ctx, tx, existing, cmd.RequestHash, cmd.IdempotencyKey)
			if err != nil {
				return err
			}
			result = replayed
			return nil
		}

		var acct Account
		var raw []byte
		err = tx.QueryRow(ctx, `
            INSERT INTO accounts (id, currency, kind, allow_overdraft, metadata)
            VALUES ($1, $2, $3, $4, $5)
         RETURNING id, currency, kind, allow_overdraft, metadata, created_at`,
			cmd.ID, cmd.Currency, cmd.Kind, cmd.AllowOverdraft, metaJSON).
			Scan(&acct.ID, &acct.Currency, &acct.Kind, &acct.AllowOverdraft, &raw, &acct.CreatedAt)
		if storage.IsUniqueViolation(err, "") {
			return apperr.New(apperr.CodeAccountExists, "account %q already exists", cmd.ID).
				WithDetail("account_id", cmd.ID)
		}
		if err != nil {
			return fmt.Errorf("ledger: insert account: %w", err)
		}
		if err := json.Unmarshal(raw, &acct.Metadata); err != nil {
			acct.Metadata = map[string]any{}
		}

		_, err = tx.Exec(ctx, `
            INSERT INTO balances (account_id, amount, allow_overdraft)
            VALUES ($1, 0, $2)`, cmd.ID, cmd.AllowOverdraft)
		if err != nil {
			return fmt.Errorf("ledger: insert balance: %w", err)
		}

		body, err := json.Marshal(acct)
		if err != nil {
			return apperr.Internal(err)
		}
		if err := idempotency.Complete(ctx, tx, cmd.IdempotencyScope, cmd.IdempotencyKey, 201, body, nil); err != nil {
			return err
		}
		result = &Result{ResponseStatus: 201, ResponseBody: body}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetAccount loads an account by ID.
func (s *Service) GetAccount(ctx context.Context, id string) (*Account, error) {
	var acct Account
	var raw []byte
	err := s.db.Pool().QueryRow(ctx, `
        SELECT id, currency, kind, allow_overdraft, metadata, created_at
          FROM accounts WHERE id = $1`, id).
		Scan(&acct.ID, &acct.Currency, &acct.Kind, &acct.AllowOverdraft, &raw, &acct.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, apperr.UnknownAccount(id)
	}
	if err != nil {
		return nil, fmt.Errorf("ledger: get account: %w", err)
	}
	if err := json.Unmarshal(raw, &acct.Metadata); err != nil {
		acct.Metadata = map[string]any{}
	}
	return &acct, nil
}

// GetBalance returns the authoritative balance of an account.
func (s *Service) GetBalance(ctx context.Context, id string) (*Balance, error) {
	var b Balance
	err := s.db.Pool().QueryRow(ctx, `
        SELECT b.account_id, b.amount, a.currency, b.version, b.updated_at
          FROM balances b JOIN accounts a ON a.id = b.account_id
         WHERE b.account_id = $1`, id).
		Scan(&b.AccountID, &b.Amount, &b.Currency, &b.Version, &b.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, apperr.UnknownAccount(id)
	}
	if err != nil {
		return nil, fmt.Errorf("ledger: get balance: %w", err)
	}
	return &b, nil
}

// GetTransaction loads a transaction and its entries.
func (s *Service) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	t, err := loadTransaction(ctx, s.db.Pool(), id)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func loadTransaction(ctx context.Context, q storage.Querier, id string) (*Transaction, error) {
	var t Transaction
	err := q.QueryRow(ctx, `
        SELECT t.id::text, t.kind, t.currency, t.description, t.external_reference,
               t.reverses_transaction_id::text,
               (SELECT r.id::text FROM ledger_transactions r
                 WHERE r.reverses_transaction_id = t.id) AS reversed_by,
               t.created_at
          FROM ledger_transactions t
         WHERE t.id = $1`, id).
		Scan(&t.ID, &t.Kind, &t.Currency, &t.Description, &t.ExternalReference,
			&t.ReversesTransactionID, &t.ReversedByTransactionID, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, apperr.New(apperr.CodeTransactionNotFound, "transaction %q does not exist", id).
			WithDetail("transaction_id", id)
	}
	if err != nil {
		// An invalid UUID reaches the driver as a syntax error; report it as a
		// client mistake rather than a 500.
		if strings.Contains(err.Error(), "invalid input syntax for type uuid") {
			return nil, apperr.InvalidRequest("transaction id must be a UUID")
		}
		return nil, fmt.Errorf("ledger: get transaction: %w", err)
	}

	rows, err := q.Query(ctx, `
        SELECT id::text, transaction_id::text, account_id, amount, currency, seq, created_at
          FROM ledger_entries WHERE transaction_id = $1 ORDER BY seq`, id)
	if err != nil {
		return nil, fmt.Errorf("ledger: get entries: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.Amount, &e.Currency, &e.Seq, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ledger: scan entry: %w", err)
		}
		t.Entries = append(t.Entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &t, nil
}

// ListEntries returns an account's entry history, newest first, using keyset
// pagination so that concurrent writes cannot cause skipped or repeated rows.
func (s *Service) ListEntries(ctx context.Context, accountID string, limit int, cursor string) (*EntryPage, error) {
	if _, err := s.GetAccount(ctx, accountID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	var (
		cursorTime time.Time
		cursorID   string
		hasCursor  bool
	)
	if cursor != "" {
		var err error
		cursorTime, cursorID, err = decodeCursor(cursor)
		if err != nil {
			return nil, apperr.InvalidRequest("invalid cursor")
		}
		hasCursor = true
	}

	rows, err := s.db.Pool().Query(ctx, `
        SELECT id::text, transaction_id::text, account_id, amount, currency, seq, created_at
          FROM ledger_entries
         WHERE account_id = $1
           AND (NOT $4::bool OR (created_at, id) < ($2::timestamptz, $3::uuid))
         ORDER BY created_at DESC, id DESC
         LIMIT $5`, accountID, cursorTime, nullUUID(cursorID), hasCursor, limit+1)
	if err != nil {
		return nil, fmt.Errorf("ledger: list entries: %w", err)
	}
	defer rows.Close()

	page := &EntryPage{Entries: []Entry{}}
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.Amount, &e.Currency, &e.Seq, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("ledger: scan entry: %w", err)
		}
		page.Entries = append(page.Entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(page.Entries) > limit {
		last := page.Entries[limit-1]
		page.Entries = page.Entries[:limit]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

func encodeCursor(ts time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(ts.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", err
	}
	tsStr, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, "", errors.New("ledger: malformed cursor")
	}
	ts, err := time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		return time.Time{}, "", err
	}
	return ts, id, nil
}

func nullUUID(id string) any {
	if id == "" {
		return nil
	}
	return id
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// listAccountsForTransaction returns the distinct accounts touched by a
// transaction, used to compute the lock set for a reversal.
func listAccountsForTransaction(ctx context.Context, tx pgx.Tx, transactionID string) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT DISTINCT account_id FROM ledger_entries WHERE transaction_id = $1`, transactionID)
	if err != nil {
		return nil, fmt.Errorf("ledger: list transaction accounts: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
