// Package reconciliation recomputes balances from the immutable ledger
// entries and reports any disagreement.
//
// This is the audit that makes the whole design falsifiable: balances are a
// cached projection, entries are the truth. If they ever diverge, something is
// wrong and this reports it rather than hiding it.
package reconciliation

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/internal/observability"
)

// Mismatch is a single account whose stored balance disagrees with its
// entries.
type Mismatch struct {
	AccountID string `json:"account_id"`
	Stored    int64  `json:"stored_balance"`
	Computed  int64  `json:"computed_balance"`
	Delta     int64  `json:"delta"`
}

// Report is the outcome of a reconciliation run.
type Report struct {
	RanAt               time.Time  `json:"ran_at"`
	AccountsChecked     int        `json:"accounts_checked"`
	TransactionsChecked int        `json:"transactions_checked"`
	Mismatches          []Mismatch `json:"mismatches"`
	UnbalancedTxIDs     []string   `json:"unbalanced_transaction_ids"`
	Duration            string     `json:"duration"`
}

// OK reports whether the ledger is internally consistent.
func (r *Report) OK() bool { return len(r.Mismatches) == 0 && len(r.UnbalancedTxIDs) == 0 }

// Reconciler runs consistency checks against PostgreSQL.
type Reconciler struct {
	pool    *pgxpool.Pool
	metrics *observability.Metrics
}

// New builds a reconciler.
func New(pool *pgxpool.Pool, metrics *observability.Metrics) *Reconciler {
	return &Reconciler{pool: pool, metrics: metrics}
}

// Run checks that every balance equals the sum of that account's entries
// (INVARIANT 3) and that every transaction sums to zero (INVARIANT 1).
func (r *Reconciler) Run(ctx context.Context) (*Report, error) {
	start := time.Now()
	report := &Report{RanAt: start.UTC(), Mismatches: []Mismatch{}, UnbalancedTxIDs: []string{}}

	// The LEFT JOIN matters: an account with no entries must reconcile to
	// zero, not be skipped.
	rows, err := r.pool.Query(ctx, `
        SELECT b.account_id, b.amount, COALESCE(SUM(e.amount), 0) AS computed
          FROM balances b
          LEFT JOIN ledger_entries e ON e.account_id = b.account_id
         GROUP BY b.account_id, b.amount`)
	if err != nil {
		r.metrics.ReconciliationRuns.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("reconciliation: balances: %w", err)
	}
	for rows.Next() {
		var m Mismatch
		if err := rows.Scan(&m.AccountID, &m.Stored, &m.Computed); err != nil {
			rows.Close()
			return nil, fmt.Errorf("reconciliation: scan balance: %w", err)
		}
		report.AccountsChecked++
		if m.Stored != m.Computed {
			m.Delta = m.Stored - m.Computed
			report.Mismatches = append(report.Mismatches, m)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reconciliation: balances: %w", err)
	}

	txRows, err := r.pool.Query(ctx, `
        SELECT t.id::text, COALESCE(SUM(e.amount), 0) AS total, COUNT(e.id) AS entry_count
          FROM ledger_transactions t
          LEFT JOIN ledger_entries e ON e.transaction_id = t.id
         GROUP BY t.id`)
	if err != nil {
		r.metrics.ReconciliationRuns.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("reconciliation: transactions: %w", err)
	}
	for txRows.Next() {
		var id string
		var total int64
		var entryCount int
		if err := txRows.Scan(&id, &total, &entryCount); err != nil {
			txRows.Close()
			return nil, fmt.Errorf("reconciliation: scan transaction: %w", err)
		}
		report.TransactionsChecked++
		if total != 0 || entryCount < 2 {
			report.UnbalancedTxIDs = append(report.UnbalancedTxIDs, id)
		}
	}
	txRows.Close()
	if err := txRows.Err(); err != nil {
		return nil, fmt.Errorf("reconciliation: transactions: %w", err)
	}

	report.Duration = time.Since(start).String()

	mismatchCount := len(report.Mismatches) + len(report.UnbalancedTxIDs)
	if mismatchCount > 0 {
		r.metrics.ReconciliationMismatches.Add(float64(mismatchCount))
		r.metrics.ReconciliationRuns.WithLabelValues("mismatch").Inc()
		observability.Logger(ctx).Error("reconciliation found inconsistencies",
			"balance_mismatches", len(report.Mismatches),
			"unbalanced_transactions", len(report.UnbalancedTxIDs))
	} else {
		r.metrics.ReconciliationRuns.WithLabelValues("ok").Inc()
		observability.Logger(ctx).Info("reconciliation clean",
			"accounts", report.AccountsChecked, "transactions", report.TransactionsChecked)
	}
	return report, nil
}
