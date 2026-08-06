package ledger

import (
	"testing"

	"github.com/adityarangi/payment-ledger/internal/apperr"
)

func TestTransferCommandValidate(t *testing.T) {
	valid := TransferCommand{
		SourceAccountID:      "a",
		DestinationAccountID: "b",
		Amount:               100,
		Currency:             "USD",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*TransferCommand)
	}{
		{"zero amount", func(c *TransferCommand) { c.Amount = 0 }},
		{"negative amount", func(c *TransferCommand) { c.Amount = -1 }},
		{"missing source", func(c *TransferCommand) { c.SourceAccountID = "" }},
		{"missing destination", func(c *TransferCommand) { c.DestinationAccountID = "" }},
		{"self transfer", func(c *TransferCommand) { c.DestinationAccountID = c.SourceAccountID }},
		{"lowercase currency", func(c *TransferCommand) { c.Currency = "usd" }},
		{"long currency", func(c *TransferCommand) { c.Currency = "USDT" }},
		{"empty currency", func(c *TransferCommand) { c.Currency = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := valid
			tc.mutate(&cmd)
			err := cmd.Validate()
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !apperr.Is(err, apperr.CodeInvalidRequest) {
				t.Fatalf("error code = %v, want invalid_request", apperr.From(err).Code)
			}
		})
	}
}

func TestCreateAccountCommandValidate(t *testing.T) {
	valid := CreateAccountCommand{ID: "acct-1", Currency: "USD", Kind: KindUser}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid command rejected: %v", err)
	}

	// Only system accounts may run negative; a user account that could
	// overdraw would break the non-negative balance invariant by design.
	overdraftUser := valid
	overdraftUser.AllowOverdraft = true
	if err := overdraftUser.Validate(); err == nil {
		t.Fatal("a user account was allowed to enable overdraft")
	}

	system := CreateAccountCommand{ID: "sys", Currency: "USD", Kind: KindSystem, AllowOverdraft: true}
	if err := system.Validate(); err != nil {
		t.Fatalf("system overdraft account rejected: %v", err)
	}

	for _, id := range []string{"", "has space", "-leading", "a/b"} {
		cmd := valid
		cmd.ID = id
		if err := cmd.Validate(); err == nil {
			t.Fatalf("account id %q was accepted", id)
		}
	}
}

func TestTransactionSum(t *testing.T) {
	txn := Transaction{Entries: []Entry{
		{Amount: -2500}, {Amount: 2500},
	}}
	if got := txn.Sum(); got != 0 {
		t.Fatalf("Sum() = %d, want 0", got)
	}

	unbalanced := Transaction{Entries: []Entry{{Amount: -2500}, {Amount: 2400}}}
	if got := unbalanced.Sum(); got != -100 {
		t.Fatalf("Sum() = %d, want -100", got)
	}
}

// Balance updates must follow the same order as the locks that protect them,
// otherwise two transfers could acquire row locks in opposite orders.
func TestSortedEntriesByAccount(t *testing.T) {
	entries := []Entry{
		{AccountID: "zeta", Amount: -100},
		{AccountID: "alpha", Amount: 100},
	}
	sorted := sortedEntriesByAccount(entries)

	if sorted[0].AccountID != "alpha" || sorted[1].AccountID != "zeta" {
		t.Fatalf("entries not sorted by account id: %v", sorted)
	}
	// The input must not be mutated.
	if entries[0].AccountID != "zeta" {
		t.Fatal("sortedEntriesByAccount mutated its input")
	}
}

func TestReversalAmount(t *testing.T) {
	txn := &Transaction{Entries: []Entry{{Amount: -2500}, {Amount: 2500}}}
	if got := reversalAmount(txn); got != 2500 {
		t.Fatalf("reversalAmount() = %d, want 2500", got)
	}
}
