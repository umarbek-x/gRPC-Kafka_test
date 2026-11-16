// Package payment implements the payment-service: it reacts to order events,
// charges (or refunds) through a payment gateway and publishes the result.
//
// It has no synchronous API at all. It is a pure event-driven service:
//
//	orders.v1 ──► [payment-service] ──► payments.v1
package payment

import (
	"context"
	"errors"
	"time"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
)

const ServiceName = "payment-service"

type Status string

const (
	StatusCompleted Status = "COMPLETED"
	StatusFailed    Status = "FAILED"
	StatusRefunded  Status = "REFUNDED"
)

type Payment struct {
	ID            string
	OrderID       string
	CustomerID    string
	AmountMinor   int64
	Currency      string
	Status        Status
	FailureReason string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

var (
	ErrNotFound         = errors.New("payment not found")
	ErrAlreadyProcessed = errors.New("event already processed")
)

// Store is the persistence port of the payment-service.
type Store interface {
	// GetByOrder returns the payment of an order or ErrNotFound.
	GetByOrder(ctx context.Context, orderID string) (*Payment, error)
	// Record atomically stores: the consumed event ID (inbox), the payment,
	// and the outgoing events (outbox). Returns ErrAlreadyProcessed if the
	// event ID was seen before.
	Record(ctx context.Context, eventID string, p *Payment, evts []outbox.Message) error
	// MarkRefunded atomically records the event ID, flips a COMPLETED payment
	// to REFUNDED and stores the events returned by build. When the payment
	// is not COMPLETED nothing changes and refunded=false.
	MarkRefunded(ctx context.Context, eventID, orderID string, build func(p *Payment) ([]outbox.Message, error)) (refunded bool, err error)
}
