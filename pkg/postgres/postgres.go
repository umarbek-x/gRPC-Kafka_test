// Package postgres wraps pgx connection pooling, transactions and migrations.
package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

// Connect creates a connection pool and waits until the database answers.
//
// A pool keeps a set of open connections that goroutines borrow and return.
// Opening a TCP+TLS+auth connection per query would be far too slow.
// MaxConns caps how many connections this service can hold; size it so that
// (instances × MaxConns) stays below Postgres' max_connections.
func Connect(ctx context.Context, dsn string, maxConns int32, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	// Containers start in any order: retry until the DB accepts connections.
	_, err = retry.Do(ctx, retry.Policy{MaxAttempts: 10, Initial: 500 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2},
		func(ctx context.Context) error { return pool.Ping(ctx) },
		func(attempt int, err error, wait time.Duration) {
			log.Warn("postgres not ready, retrying", "attempt", attempt, "wait", wait.String(), "err", err)
		})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// InTx runs fn inside a transaction. It commits if fn returns nil and rolls
// back otherwise (including on panic). This removes the most common
// transaction bug: forgetting to roll back on an error path.
func InTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) (err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// IsUniqueViolation reports whether err is a Postgres unique constraint error.
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Migrate applies *.sql files from dir in lexical order (0001_..., 0002_...).
// Applied versions are recorded in schema_migrations. A Postgres advisory
// lock guarantees that when several replicas start at once, only one runs
// the migrations.
//
// (Production teams usually use a tool such as goose or golang-migrate,
// which add down-migrations and a CLI. The core idea is exactly this.)
func Migrate(ctx context.Context, pool *pgxpool.Pool, files embed.FS, dir string, log *slog.Logger) error {
	const lockID = 7_301_995 // arbitrary app-wide constant
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, lockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockID) }()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(files, dir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sqlBytes, err := fs.ReadFile(files, dir+"/"+name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		log.Info("applied migration", "version", name)
	}
	return nil
}
