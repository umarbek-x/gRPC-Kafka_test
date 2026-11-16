// Package repository is the Postgres ADAPTER for service.Repository.
package repository

import (
	"context"
	"embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/service"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/postgres"
)

// Migrations are compiled into the binary, so a deploy is one file.
//
//go:embed migrations/*.sql
var Migrations embed.FS

const MigrationsDir = "migrations"

type Postgres struct {
	pool *pgxpool.Pool
}

// Compile-time proof that *Postgres implements the port.
var _ service.Repository = (*Postgres)(nil)

func New(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

const orderColumns = `id::text, customer_id, status, currency, total_minor, failure_reason,
	COALESCE(idempotency_key, ''), version, created_at, updated_at`

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so helpers work
// inside and outside transactions.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func scanOrder(row pgx.Row) (*domain.Order, error) {
	var o domain.Order
	var status string
	if err := row.Scan(&o.ID, &o.CustomerID, &status, &o.Currency, &o.TotalMinor, &o.FailureReason,
		&o.IdempotencyKey, &o.Version, &o.CreatedAt, &o.UpdatedAt); err != nil {
		return nil, err
	}
	o.Status = domain.Status(status)
	o.CreatedAt, o.UpdatedAt = o.CreatedAt.UTC(), o.UpdatedAt.UTC()
	return &o, nil
}

// loadItems fetches the items of MANY orders in ONE query and attaches them.
// The naive alternative, one query per order, is the classic "N+1 queries"
// performance bug.
func loadItems(ctx context.Context, q querier, orders ...*domain.Order) error {
	if len(orders) == 0 {
		return nil
	}
	byID := make(map[string]*domain.Order, len(orders))
	ids := make([]string, len(orders))
	for i, o := range orders {
		byID[o.ID] = o
		ids[i] = o.ID
	}
	rows, err := q.Query(ctx,
		`SELECT order_id::text, product_id, quantity, unit_price_minor
		   FROM order_items
		  WHERE order_id = ANY($1::text[]::uuid[])
		  ORDER BY order_id, line_no`, ids)
	if err != nil {
		return fmt.Errorf("load items: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var orderID string
		var it domain.Item
		if err := rows.Scan(&orderID, &it.ProductID, &it.Quantity, &it.UnitPriceMinor); err != nil {
			return err
		}
		byID[orderID].Items = append(byID[orderID].Items, it)
	}
	return rows.Err()
}

func (r *Postgres) Create(ctx context.Context, o *domain.Order, evts []outbox.Message) (*domain.Order, bool, error) {
	var idemKey *string // nil -> SQL NULL
	if o.IdempotencyKey != "" {
		idemKey = &o.IdempotencyKey
	}
	created := false
	err := postgres.InTx(ctx, r.pool, func(tx pgx.Tx) error {
		// ON CONFLICT DO NOTHING turns "duplicate key" into "0 rows inserted"
		// without aborting the transaction. Two concurrent retries are safe:
		// the unique constraint lets exactly one of them insert.
		tag, err := tx.Exec(ctx,
			`INSERT INTO orders (id, customer_id, status, currency, total_minor, failure_reason,
			                     idempotency_key, version, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			 ON CONFLICT ON CONSTRAINT orders_idempotency_uniq DO NOTHING`,
			o.ID, o.CustomerID, string(o.Status), o.Currency, o.TotalMinor, o.FailureReason,
			idemKey, o.Version, o.CreatedAt, o.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil // idempotent replay: write nothing, emit nothing
		}
		created = true

		// Batch: send all item inserts in one network round trip.
		batch := &pgx.Batch{}
		for i, it := range o.Items {
			batch.Queue(`INSERT INTO order_items (order_id, line_no, product_id, quantity, unit_price_minor)
			             VALUES ($1,$2,$3,$4,$5)`, o.ID, i, it.ProductID, it.Quantity, it.UnitPriceMinor)
		}
		br := tx.SendBatch(ctx, batch)
		for range o.Items {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return fmt.Errorf("insert order item: %w", err)
			}
		}
		if err := br.Close(); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, evts...)
	})
	if err != nil {
		return nil, false, err
	}
	if created {
		return o.Clone(), true, nil
	}

	existing, err := scanOrder(r.pool.QueryRow(ctx,
		`SELECT `+orderColumns+` FROM orders WHERE customer_id = $1 AND idempotency_key = $2`,
		o.CustomerID, o.IdempotencyKey))
	if err != nil {
		return nil, false, fmt.Errorf("load existing order for idempotency key: %w", err)
	}
	if err := loadItems(ctx, r.pool, existing); err != nil {
		return nil, false, err
	}
	return existing, false, nil
}

func (r *Postgres) Get(ctx context.Context, id string) (*domain.Order, error) {
	o, err := scanOrder(r.pool.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get order: %w", err)
	}
	if err := loadItems(ctx, r.pool, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (r *Postgres) List(ctx context.Context, customerID string, after *service.Cursor, limit int) ([]*domain.Order, error) {
	var rows pgx.Rows
	var err error
	if after == nil {
		rows, err = r.pool.Query(ctx,
			`SELECT `+orderColumns+` FROM orders
			  WHERE customer_id = $1
			  ORDER BY created_at DESC, id DESC LIMIT $2`, customerID, limit)
	} else {
		// Row-value comparison (a, b) < (x, y) expresses "strictly after the
		// cursor" for a two-column sort and can use the composite index.
		rows, err = r.pool.Query(ctx,
			`SELECT `+orderColumns+` FROM orders
			  WHERE customer_id = $1 AND (created_at, id) < ($2, $3::text::uuid)
			  ORDER BY created_at DESC, id DESC LIMIT $4`, customerID, after.CreatedAt, after.ID, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()

	var orders []*domain.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := loadItems(ctx, r.pool, orders...); err != nil {
		return nil, err
	}
	return orders, nil
}

func (r *Postgres) Update(ctx context.Context, id string, inbox *service.InboxKey, fn service.UpdateFunc) (*domain.Order, error) {
	var out *domain.Order
	err := postgres.InTx(ctx, r.pool, func(tx pgx.Tx) error {
		if inbox != nil {
			// Idempotent consumer: the primary key rejects a second insert of
			// the same event ID. Because it is in the same transaction as the
			// state change, "processed" and "applied" can never disagree.
			tag, err := tx.Exec(ctx,
				`INSERT INTO inbox (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
				inbox.Consumer, inbox.EventID)
			if err != nil {
				return fmt.Errorf("insert inbox: %w", err)
			}
			if tag.RowsAffected() == 0 {
				return domain.ErrAlreadyProcessed
			}
		}

		// FOR UPDATE takes a row lock until commit: two concurrent updates of
		// the same order run one after the other (pessimistic locking), so
		// the state machine check below always sees the latest state.
		o, err := scanOrder(tx.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE id = $1 FOR UPDATE`, id))
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("lock order: %w", err)
		}
		if err := loadItems(ctx, tx, o); err != nil {
			return err
		}

		evts, err := fn(o)
		if err != nil {
			return err // rollback (also un-does the inbox insert)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE orders SET status = $2, failure_reason = $3, version = $4, updated_at = $5 WHERE id = $1`,
			o.ID, string(o.Status), o.FailureReason, o.Version, o.UpdatedAt); err != nil {
			return fmt.Errorf("update order: %w", err)
		}
		if err := outbox.Insert(ctx, tx, evts...); err != nil {
			return err
		}
		out = o
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
