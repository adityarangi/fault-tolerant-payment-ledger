// Package ledger implements the append-only double-entry ledger and the
// single PostgreSQL transaction that makes a payment atomic.
package ledger

import (
	"regexp"
	"time"

	"github.com/adityarangi/payment-ledger/internal/apperr"
)

// Account kinds.
const (
	KindUser   = "user"
	KindSystem = "system"
)

// Transaction kinds.
const (
	TxKindTransfer = "transfer"
	TxKindFunding  = "funding"
	TxKindReversal = "reversal"
)

// Event types emitted to the outbox.
const (
	EventPaymentCreated  = "payment.created"
	EventPaymentReversed = "payment.reversed"
	EventPaymentFailed   = "payment.failed"
)

// Account is a ledger account. Currency is fixed at creation.
type Account struct {
	ID             string         `json:"id"`
	Currency       string         `json:"currency"`
	Kind           string         `json:"kind"`
	AllowOverdraft bool           `json:"allow_overdraft"`
	Metadata       map[string]any `json:"metadata"`
	CreatedAt      time.Time      `json:"created_at"`
}

// Balance is the authoritative balance of an account, in minor units.
type Balance struct {
	AccountID string    `json:"account_id"`
	Amount    int64     `json:"amount"`
	Currency  string    `json:"currency"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Entry is one side of a double-entry posting. A positive amount credits the
// account, a negative amount debits it. Entries are immutable.
type Entry struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	Seq           int       `json:"seq"`
	CreatedAt     time.Time `json:"created_at"`
}

// Transaction is a balanced set of entries committed atomically.
type Transaction struct {
	ID                      string    `json:"id"`
	Kind                    string    `json:"kind"`
	Currency                string    `json:"currency"`
	Description             string    `json:"description"`
	ExternalReference       *string   `json:"external_reference,omitempty"`
	ReversesTransactionID   *string   `json:"reverses_transaction_id,omitempty"`
	ReversedByTransactionID *string   `json:"reversed_by_transaction_id,omitempty"`
	CreatedAt               time.Time `json:"created_at"`
	Entries                 []Entry   `json:"entries"`
}

// Sum returns the total of the transaction's entries; it must be zero.
func (t *Transaction) Sum() int64 {
	var total int64
	for _, e := range t.Entries {
		total += e.Amount
	}
	return total
}

// CreateAccountCommand creates a new account.
type CreateAccountCommand struct {
	ID             string
	Currency       string
	Kind           string
	AllowOverdraft bool
	Metadata       map[string]any

	IdempotencyScope string
	IdempotencyKey   string
	RequestHash      string
}

// TransferCommand moves money between two accounts.
type TransferCommand struct {
	SourceAccountID      string
	DestinationAccountID string
	Amount               int64
	Currency             string
	Description          string
	ExternalReference    string

	// Idempotency scope/key/hash are carried through so that the ledger write
	// and the idempotency record commit together.
	IdempotencyScope string
	IdempotencyKey   string
	RequestHash      string
}

// ReverseCommand fully reverses a committed transaction.
type ReverseCommand struct {
	TransactionID string
	Reason        string

	IdempotencyScope string
	IdempotencyKey   string
	RequestHash      string
}

// EntryPage is a page of account history.
type EntryPage struct {
	Entries    []Entry `json:"entries"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

var (
	currencyRe  = regexp.MustCompile(`^[A-Z]{3}$`)
	accountIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// Validate checks a create-account command.
func (c *CreateAccountCommand) Validate() error {
	if !accountIDRe.MatchString(c.ID) {
		return apperr.InvalidRequest("id must match %s", accountIDRe.String())
	}
	if !currencyRe.MatchString(c.Currency) {
		return apperr.InvalidRequest("currency must be a 3-letter uppercase ISO 4217 code")
	}
	if c.Kind != KindUser && c.Kind != KindSystem {
		return apperr.InvalidRequest("kind must be %q or %q", KindUser, KindSystem)
	}
	if c.AllowOverdraft && c.Kind != KindSystem {
		return apperr.InvalidRequest("only system accounts may allow overdraft")
	}
	return nil
}

// Validate checks a transfer command. Currency agreement with the accounts
// themselves is checked later, inside the transaction, against locked rows.
func (c *TransferCommand) Validate() error {
	if c.SourceAccountID == "" || c.DestinationAccountID == "" {
		return apperr.InvalidRequest("source_account_id and destination_account_id are required")
	}
	if c.SourceAccountID == c.DestinationAccountID {
		return apperr.InvalidRequest("source_account_id and destination_account_id must differ")
	}
	if c.Amount <= 0 {
		return apperr.InvalidRequest("amount must be a positive integer in minor units")
	}
	if !currencyRe.MatchString(c.Currency) {
		return apperr.InvalidRequest("currency must be a 3-letter uppercase ISO 4217 code")
	}
	if len(c.Description) > 512 {
		return apperr.InvalidRequest("description must be at most 512 characters")
	}
	if len(c.ExternalReference) > 256 {
		return apperr.InvalidRequest("external_reference must be at most 256 characters")
	}
	return nil
}
