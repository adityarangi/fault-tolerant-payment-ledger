package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityarangi/payment-ledger/internal/apperr"
	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/failpoint"
	"github.com/adityarangi/payment-ledger/internal/idempotency"
	"github.com/adityarangi/payment-ledger/internal/observability"
	"github.com/adityarangi/payment-ledger/internal/outbox"
	"github.com/adityarangi/payment-ledger/internal/storage"
)

// Service implements the ledger use cases.
type Service struct {
	db      *storage.DB
	cfg     *config.Config
	metrics *observability.Metrics
	fp      *failpoint.Registry
	now     func() time.Time
}

// NewService builds a ledger service.
func NewService(db *storage.DB, cfg *config.Config, metrics *observability.Metrics, fp *failpoint.Registry) *Service {
	return &Service{db: db, cfg: cfg, metrics: metrics, fp: fp, now: time.Now}
}

// Result carries both the domain object and the exact HTTP response that must
// be replayed for any future retry of the same idempotency key.
type Result struct {
	Transaction    *Transaction
	ResponseStatus int
	ResponseBody   json.RawMessage
	// Replayed is true when the result came from a stored idempotency record
	// rather than from a newly committed transaction.
	Replayed bool
}

// Transfer moves money between two accounts.
//
// Everything below commits in ONE PostgreSQL transaction: the ledger
// transaction header, both entries, both balance updates, the durable
// idempotency result, and the outbox event. There is no window in which a
// payment exists without its event, or an event without its payment.
func (s *Service) Transfer(ctx context.Context, cmd TransferCommand) (*Result, error) {
	if err := cmd.Validate(); err != nil {
		s.metrics.TransfersTotal.WithLabelValues("failure", string(apperr.From(err).Code)).Inc()
		return nil, err
	}

	// Failing here proves that a request rejected before BEGIN leaves nothing
	// behind at all — no ledger row, and no idempotency record to block a
	// retry.
	if err := s.fp.Eval(ctx, failpoint.BeforeTxBegin); err != nil {
		s.metrics.TransfersTotal.WithLabelValues("failure", "failpoint").Inc()
		return nil, apperr.Internal(err)
	}

	txID := uuid.NewString()
	ctx = observability.WithTransactionID(ctx, txID)

	var result *Result
	err := s.db.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		// Serialise same-key requests so concurrent retries queue rather than
		// racing. The (scope, key) primary key remains the real guarantee.
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
			return nil // commit the empty transaction; nothing was written
		}

		committed, err := s.executeTransfer(ctx, tx, txID, cmd)
		if err != nil {
			return err
		}
		result = committed
		return nil
	})
	if err != nil {
		appErr := apperr.From(err)
		s.metrics.TransfersTotal.WithLabelValues("failure", string(appErr.Code)).Inc()
		return nil, err
	}

	if result.Replayed {
		s.metrics.IdempotencyTotal.WithLabelValues("hit", "postgres").Inc()
		return result, nil
	}
	s.metrics.TransfersTotal.WithLabelValues("success", "").Inc()
	s.metrics.IdempotencyTotal.WithLabelValues("miss", "postgres").Inc()

	// The money has moved and the event is durably queued. A failure from here
	// on is a lost response, not a lost payment: the client retries with the
	// same key and gets this exact response back (INVARIANT 6).
	if err := s.fp.Eval(ctx, failpoint.AfterCommit); err != nil {
		return nil, apperr.Internal(fmt.Errorf("response lost after commit: %w", err))
	}
	return result, nil
}

// executeTransfer performs the actual postings. It runs with the idempotency
// row already claimed inside tx.
func (s *Service) executeTransfer(ctx context.Context, tx pgx.Tx, txID string, cmd TransferCommand) (*Result, error) {
	accounts, err := lockBalances(ctx, tx, []string{cmd.SourceAccountID, cmd.DestinationAccountID})
	if err != nil {
		return nil, err
	}
	if err := s.fp.Eval(ctx, failpoint.AfterAccountsLocked); err != nil {
		return nil, apperr.Internal(err)
	}

	source := accounts[cmd.SourceAccountID]
	dest := accounts[cmd.DestinationAccountID]

	if source.Currency != cmd.Currency {
		return nil, apperr.CurrencyMismatch(
			"source account %q holds %s but the transfer is in %s",
			source.ID, source.Currency, cmd.Currency).
			WithDetail("account_id", source.ID).WithDetail("account_currency", source.Currency)
	}
	if dest.Currency != cmd.Currency {
		return nil, apperr.CurrencyMismatch(
			"destination account %q holds %s but the transfer is in %s",
			dest.ID, dest.Currency, cmd.Currency).
			WithDetail("account_id", dest.ID).WithDetail("account_currency", dest.Currency)
	}
	// Checked here for a clean error; enforced for real by the CHECK
	// constraint applied during applyBalanceDelta below.
	if !source.AllowOverdraft && source.Balance < cmd.Amount {
		return nil, apperr.InsufficientFunds(source.ID).
			WithDetail("balance", source.Balance).
			WithDetail("requested", cmd.Amount)
	}

	kind := TxKindTransfer
	if source.Kind == KindSystem {
		kind = TxKindFunding
	}

	txn := &Transaction{
		ID:          txID,
		Kind:        kind,
		Currency:    cmd.Currency,
		Description: cmd.Description,
		CreatedAt:   s.now().UTC(),
		Entries: []Entry{
			{ID: uuid.NewString(), TransactionID: txID, AccountID: source.ID, Amount: -cmd.Amount, Currency: cmd.Currency, Seq: 0},
			{ID: uuid.NewString(), TransactionID: txID, AccountID: dest.ID, Amount: cmd.Amount, Currency: cmd.Currency, Seq: 1},
		},
	}
	if cmd.ExternalReference != "" {
		ref := cmd.ExternalReference
		txn.ExternalReference = &ref
	}

	if err := insertTransaction(ctx, tx, txn); err != nil {
		return nil, err
	}
	if err := insertEntries(ctx, tx, txn.Entries); err != nil {
		return nil, err
	}
	if err := s.fp.Eval(ctx, failpoint.AfterEntriesWritten); err != nil {
		return nil, apperr.Internal(err)
	}

	// Apply balances in the same ascending account order used for locking.
	for _, e := range sortedEntriesByAccount(txn.Entries) {
		if _, err := applyBalanceDelta(ctx, tx, e.AccountID, e.Amount); err != nil {
			return nil, err
		}
	}
	if err := s.fp.Eval(ctx, failpoint.AfterBalancesUpdated); err != nil {
		return nil, apperr.Internal(err)
	}

	if err := s.writeOutboxEvent(ctx, tx, txn, EventPaymentCreated, outbox.PaymentData{
		TransactionID:        txn.ID,
		Kind:                 txn.Kind,
		Currency:             txn.Currency,
		Amount:               cmd.Amount,
		SourceAccountID:      source.ID,
		DestinationAccountID: dest.ID,
		Description:          cmd.Description,
		ExternalReference:    cmd.ExternalReference,
		CreatedAt:            txn.CreatedAt,
		Entries:              entryData(txn.Entries),
	}); err != nil {
		return nil, err
	}
	if err := s.fp.Eval(ctx, failpoint.AfterOutboxWritten); err != nil {
		return nil, apperr.Internal(err)
	}

	// Reload so the response reflects exactly what the database stored.
	stored, err := loadTransaction(ctx, tx, txn.ID)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(stored)
	if err != nil {
		return nil, apperr.Internal(err)
	}
	if err := idempotency.Complete(ctx, tx, cmd.IdempotencyScope, cmd.IdempotencyKey, 201, body, &txn.ID); err != nil {
		return nil, err
	}

	// Failing here proves atomicity: no entries, no balance change, no outbox
	// row, and no idempotency record survive (INVARIANT 5).
	if err := s.fp.Eval(ctx, failpoint.BeforeCommit); err != nil {
		return nil, apperr.Internal(err)
	}

	return &Result{Transaction: stored, ResponseStatus: 201, ResponseBody: body}, nil
}

// Reverse creates a complete reversal of a committed transaction.
//
// History is preserved: the original transaction and its entries are never
// touched. The reversal is a new transaction with mirrored entries, which
// restores the financial effect while leaving a full audit trail
// (INVARIANT 9).
func (s *Service) Reverse(ctx context.Context, cmd ReverseCommand) (*Result, error) {
	if cmd.TransactionID == "" {
		return nil, apperr.InvalidRequest("transaction id is required")
	}
	if err := s.fp.Eval(ctx, failpoint.BeforeTxBegin); err != nil {
		return nil, apperr.Internal(err)
	}

	reversalID := uuid.NewString()
	ctx = observability.WithTransactionID(ctx, reversalID)

	var result *Result
	err := s.db.InTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
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

		original, err := loadTransaction(ctx, tx, cmd.TransactionID)
		if err != nil {
			return err
		}
		if original.Kind == TxKindReversal {
			return apperr.InvalidRequest("a reversal transaction cannot itself be reversed")
		}
		if original.ReversedByTransactionID != nil {
			return apperr.New(apperr.CodeTransactionAlreadyReversed,
				"transaction %q has already been reversed by %q",
				original.ID, *original.ReversedByTransactionID).
				WithDetail("transaction_id", original.ID).
				WithDetail("reversal_transaction_id", *original.ReversedByTransactionID)
		}

		accountIDs, err := listAccountsForTransaction(ctx, tx, original.ID)
		if err != nil {
			return err
		}
		if _, err := lockBalances(ctx, tx, accountIDs); err != nil {
			return err
		}
		if err := s.fp.Eval(ctx, failpoint.AfterAccountsLocked); err != nil {
			return apperr.Internal(err)
		}

		reversal := &Transaction{
			ID:                    reversalID,
			Kind:                  TxKindReversal,
			Currency:              original.Currency,
			Description:           reversalDescription(original, cmd.Reason),
			ReversesTransactionID: &original.ID,
			CreatedAt:             s.now().UTC(),
		}
		for _, e := range original.Entries {
			reversal.Entries = append(reversal.Entries, Entry{
				ID:            uuid.NewString(),
				TransactionID: reversalID,
				AccountID:     e.AccountID,
				Amount:        -e.Amount, // mirrored: the pair still sums to zero
				Currency:      e.Currency,
				Seq:           e.Seq,
			})
		}

		if err := insertTransaction(ctx, tx, reversal); err != nil {
			return err
		}
		if err := insertEntries(ctx, tx, reversal.Entries); err != nil {
			return err
		}
		if err := s.fp.Eval(ctx, failpoint.AfterEntriesWritten); err != nil {
			return apperr.Internal(err)
		}
		for _, e := range sortedEntriesByAccount(reversal.Entries) {
			if _, err := applyBalanceDelta(ctx, tx, e.AccountID, e.Amount); err != nil {
				return err
			}
		}
		if err := s.fp.Eval(ctx, failpoint.AfterBalancesUpdated); err != nil {
			return apperr.Internal(err)
		}
		if err := insertReversalRecord(ctx, tx, uuid.NewString(), original.ID, reversalID, cmd.Reason); err != nil {
			return err
		}

		amount := reversalAmount(original)
		if err := s.writeOutboxEvent(ctx, tx, reversal, EventPaymentReversed, outbox.PaymentData{
			TransactionID:         reversal.ID,
			Kind:                  reversal.Kind,
			Currency:              reversal.Currency,
			Amount:                amount,
			ReversesTransactionID: original.ID,
			Reason:                cmd.Reason,
			CreatedAt:             reversal.CreatedAt,
			Entries:               entryData(reversal.Entries),
		}); err != nil {
			return err
		}
		if err := s.fp.Eval(ctx, failpoint.AfterOutboxWritten); err != nil {
			return apperr.Internal(err)
		}

		stored, err := loadTransaction(ctx, tx, reversalID)
		if err != nil {
			return err
		}
		body, err := json.Marshal(stored)
		if err != nil {
			return apperr.Internal(err)
		}
		if err := idempotency.Complete(ctx, tx, cmd.IdempotencyScope, cmd.IdempotencyKey, 201, body, &reversalID); err != nil {
			return err
		}
		if err := s.fp.Eval(ctx, failpoint.BeforeCommit); err != nil {
			return apperr.Internal(err)
		}

		result = &Result{Transaction: stored, ResponseStatus: 201, ResponseBody: body}
		return nil
	})
	if err != nil {
		s.metrics.ReversalsTotal.WithLabelValues("failure", string(apperr.From(err).Code)).Inc()
		return nil, err
	}
	if !result.Replayed {
		s.metrics.ReversalsTotal.WithLabelValues("success", "").Inc()
	}
	if err := s.fp.Eval(ctx, failpoint.AfterCommit); err != nil {
		return nil, apperr.Internal(fmt.Errorf("response lost after commit: %w", err))
	}
	return result, nil
}

// writeOutboxEvent appends the payment event to the outbox inside tx.
func (s *Service) writeOutboxEvent(ctx context.Context, tx pgx.Tx, txn *Transaction, eventType string, data outbox.PaymentData) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return apperr.Internal(err)
	}
	envelope := outbox.Envelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		SchemaVersion: outbox.SchemaVersion,
		TransactionID: txn.ID,
		OccurredAt:    txn.CreatedAt,
		CorrelationID: observability.CorrelationID(ctx),
		Data:          payload,
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		return apperr.Internal(err)
	}

	return outbox.Insert(ctx, tx, outbox.Record{
		ID:           envelope.EventID,
		Topic:        s.cfg.TopicForEvent(eventType),
		EventType:    eventType,
		AggregateID:  txn.ID,
		PartitionKey: txn.ID, // per-transaction ordering on the Kafka partition
		Payload:      body,
		Headers: map[string]string{
			"event_type":     eventType,
			"correlation_id": observability.CorrelationID(ctx),
			"schema_version": fmt.Sprintf("%d", outbox.SchemaVersion),
		},
	})
}

// replayResult turns a stored idempotency record into a Result, or returns the
// appropriate conflict error.
func replayResult(ctx context.Context, tx pgx.Tx, rec *idempotency.Record, requestHash, key string) (*Result, error) {
	if rec.RequestHash != requestHash {
		return nil, idempotency.Conflict(key)
	}
	if rec.State != idempotency.StateCompleted {
		// Only reachable if a record were committed mid-flight, which this
		// design never does; kept as a defensive branch.
		return nil, idempotency.InProgress(key)
	}

	result := &Result{
		ResponseStatus: rec.ResponseStatus,
		ResponseBody:   rec.ResponseBody,
		Replayed:       true,
	}
	if rec.TransactionID != nil {
		txn, err := loadTransaction(ctx, tx, *rec.TransactionID)
		if err != nil {
			return nil, err
		}
		result.Transaction = txn
	}
	return result, nil
}

// sortedEntriesByAccount orders entries by account ID so balance updates
// follow the same order as the locks that protect them.
func sortedEntriesByAccount(entries []Entry) []Entry {
	out := make([]Entry, len(entries))
	copy(out, entries)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].AccountID < out[j-1].AccountID; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func entryData(entries []Entry) []outbox.EntryData {
	out := make([]outbox.EntryData, 0, len(entries))
	for _, e := range entries {
		out = append(out, outbox.EntryData{
			EntryID:   e.ID,
			AccountID: e.AccountID,
			Amount:    e.Amount,
			Currency:  e.Currency,
			Seq:       e.Seq,
		})
	}
	return out
}

// reversalAmount reports the gross amount moved by a transaction, i.e. the sum
// of its positive entries.
func reversalAmount(t *Transaction) int64 {
	var total int64
	for _, e := range t.Entries {
		if e.Amount > 0 {
			total += e.Amount
		}
	}
	return total
}

func reversalDescription(original *Transaction, reason string) string {
	if reason != "" {
		return fmt.Sprintf("Reversal of %s: %s", original.ID, reason)
	}
	return fmt.Sprintf("Reversal of %s", original.ID)
}
