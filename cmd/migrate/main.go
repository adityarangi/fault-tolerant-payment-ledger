// Command migrate applies or reverts schema migrations.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adityarangi/payment-ledger/internal/config"
	"github.com/adityarangi/payment-ledger/internal/storage"
)

func main() {
	var (
		direction = flag.String("direction", "up", "up, down or status")
		steps     = flag.Int("steps", 0, "number of migrations to revert (0 = all, down only)")
	)
	flag.Parse()

	cfg, err := config.Load("migrate")
	if err != nil {
		fail(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fail(fmt.Errorf("connect: %w", err))
	}
	defer pool.Close()

	// Wait briefly for PostgreSQL: in docker compose the database may still be
	// accepting connections when this container starts.
	if err := waitForDB(ctx, pool); err != nil {
		fail(err)
	}

	switch *direction {
	case "up":
		applied, err := storage.MigrateUp(ctx, pool)
		if err != nil {
			fail(err)
		}
		if len(applied) == 0 {
			fmt.Println("migrate: schema already up to date")
			return
		}
		fmt.Printf("migrate: applied %v\n", applied)

	case "down":
		reverted, err := storage.MigrateDown(ctx, pool, *steps)
		if err != nil {
			fail(err)
		}
		fmt.Printf("migrate: reverted %v\n", reverted)

	case "status":
		versions, err := storage.AppliedVersions(ctx, pool)
		if err != nil {
			fail(err)
		}
		fmt.Printf("migrate: applied versions %v\n", versions)

	default:
		fail(fmt.Errorf("unknown direction %q (want up, down or status)", *direction))
	}
}

func waitForDB(ctx context.Context, pool *pgxpool.Pool) error {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := pool.Ping(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("database not reachable: %w", lastErr)
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
	os.Exit(1)
}
