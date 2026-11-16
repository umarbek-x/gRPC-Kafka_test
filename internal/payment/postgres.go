package payment

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/postgres"
)

//go:embed migrations/*.sql
var Migrations embed.FS

const MigrationsDir = "migrations"

type PostgresStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*PostgresStore)(nil)

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore { return &PostgresStore{pool: pool} }

const paymentColumns = `id::text, order_id::text, customer_id, amount_minor, currency, status, failure_reason, created_at, updated_at`

func scanPayment(row pgx.Row) (*Payment, error) {
	var p Payment
	var status string
	err := row.Scan(&p.ID, &p.OrderID, &p.CustomerID, &p.AmountMinor, &p.Currency, &status, &p.FailureReason, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	p.Status = Status(status)
	return &p, nil
}

func (s *PostgresStore) GetByOrder(ctx context.Context, orderID string) (*Payment, error) {
	p, err := scanPayment(s.pool.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE order_id = $1`, orderID))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("get payment: %w", err)
	}
	return p, err
}

// insertInbox returns ErrAlreadyProcessed when the event was seen before.
func insertInbox(ctx context.Context, tx pgx.Tx, eventID string) error {
	tag, err := tx.Exec(ctx, `INSERT INTO inbox (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, ServiceName, eventID)
	if err != nil {
		return fmt.Errorf("insert inbox: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrAlreadyProcessed
	}
	return nil
}

func (s *PostgresStore) Record(ctx context.Context, eventID string, p *Payment, evts []outbox.Message) error {
	return postgres.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := insertInbox(ctx, tx, eventID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO payments (id, order_id, customer_id, amount_minor, currency, status, failure_reason, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			p.ID, p.OrderID, p.CustomerID, p.AmountMinor, p.Currency, string(p.Status), p.FailureReason, p.CreatedAt, p.UpdatedAt)
		if postgres.IsUniqueViolation(err) {
			// Another delivery of a *different* event for the same order won
			// the race. The order is paid exactly once: treat as processed.
			return ErrAlreadyProcessed
		}
		if err != nil {
			return fmt.Errorf("insert payment: %w", err)
		}
		return outbox.Insert(ctx, tx, evts...)
	})
}

func (s *PostgresStore) MarkRefunded(ctx context.Context, eventID, orderID string, build func(p *Payment) ([]outbox.Message, error)) (bool, error) {
	refunded := false
	err := postgres.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := insertInbox(ctx, tx, eventID); err != nil {
			return err
		}
		p, err := scanPayment(tx.QueryRow(ctx, `SELECT `+paymentColumns+` FROM payments WHERE order_id = $1 FOR UPDATE`, orderID))
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if p.Status != StatusCompleted {
			return nil
		}
		evts, err := build(p)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE payments SET status = $2, updated_at = $3 WHERE id = $1`,
			p.ID, string(StatusRefunded), time.Now().UTC()); err != nil {
			return fmt.Errorf("update payment: %w", err)
		}
		refunded = true
		return outbox.Insert(ctx, tx, evts...)
	})
	return refunded, err
}
