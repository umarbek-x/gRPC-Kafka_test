// Package domain contains the business rules of orders.
//
// It has NO dependencies on gRPC, Kafka or Postgres. That is the core idea of
// clean/hexagonal architecture: the business rules are plain Go that can be
// unit-tested in microseconds, and the infrastructure depends on the domain,
// never the other way round.
package domain

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// Status is the lifecycle state of an order.
type Status string

const (
	StatusPending       Status = "PENDING"
	StatusPaid          Status = "PAID"
	StatusPaymentFailed Status = "PAYMENT_FAILED"
	StatusCancelled     Status = "CANCELLED"
)

// transitions is the order state machine: which states can follow which.
//
//	            ┌────────► PAID ─────────┐
//	PENDING ────┼────────► PAYMENT_FAILED│
//	            └────────► CANCELLED ◄───┘
//
// Keeping it as data (a map) instead of scattered if-statements makes the
// rules visible in one place and trivially testable.
var transitions = map[Status][]Status{
	StatusPending: {StatusPaid, StatusPaymentFailed, StatusCancelled},
	StatusPaid:    {StatusCancelled},
}

// IsFinal reports whether no further transition is possible.
func (s Status) IsFinal() bool { return len(transitions[s]) == 0 }

func (s Status) canBecome(next Status) bool {
	for _, allowed := range transitions[s] {
		if allowed == next {
			return true
		}
	}
	return false
}

// Sentinel errors. Upper layers map them to gRPC codes with errors.Is.
var (
	ErrNotFound          = errors.New("order not found")
	ErrInvalidTransition = errors.New("invalid status transition")
	ErrAlreadyProcessed  = errors.New("event already processed")
)

// ValidationError describes one invalid input field.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Message }

func invalid(field, format string, args ...any) error {
	return &ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// Limits protect the system from absurd or malicious input.
const (
	MaxItems       = 100
	MaxQuantity    = 1000
	MaxFieldLength = 128
)

// Item is one line of an order.
type Item struct {
	ProductID      string
	Quantity       int32
	UnitPriceMinor int64 // money in minor units (cents); never float
}

// Order is the aggregate root.
type Order struct {
	ID             string
	CustomerID     string
	Items          []Item
	TotalMinor     int64
	Currency       string
	Status         Status
	FailureReason  string
	IdempotencyKey string
	Version        int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewOrder validates input and builds a PENDING order. The constructor is the
// only way to create an order, so an invalid Order cannot exist.
func NewOrder(id, customerID, currency, idempotencyKey string, items []Item, now time.Time) (*Order, error) {
	if customerID == "" {
		return nil, invalid("customer_id", "must not be empty")
	}
	if len(customerID) > MaxFieldLength {
		return nil, invalid("customer_id", "must be at most %d characters", MaxFieldLength)
	}
	if len(idempotencyKey) > MaxFieldLength {
		return nil, invalid("idempotency_key", "must be at most %d characters", MaxFieldLength)
	}
	if !validCurrency(currency) {
		return nil, invalid("currency", "must be a 3-letter ISO-4217 code like USD")
	}
	if len(items) == 0 {
		return nil, invalid("items", "must contain at least one item")
	}
	if len(items) > MaxItems {
		return nil, invalid("items", "must contain at most %d items", MaxItems)
	}

	var total int64
	copied := make([]Item, len(items)) // defensive copy: caller can't mutate our state
	for i, it := range items {
		field := fmt.Sprintf("items[%d]", i)
		if it.ProductID == "" || len(it.ProductID) > MaxFieldLength {
			return nil, invalid(field+".product_id", "must be 1..%d characters", MaxFieldLength)
		}
		if it.Quantity < 1 || it.Quantity > MaxQuantity {
			return nil, invalid(field+".quantity", "must be between 1 and %d", MaxQuantity)
		}
		if it.UnitPriceMinor < 0 {
			return nil, invalid(field+".unit_price_minor", "must not be negative")
		}
		// Overflow check BEFORE multiplying/adding: int64 silently wraps in Go.
		if it.UnitPriceMinor > 0 && int64(it.Quantity) > math.MaxInt64/it.UnitPriceMinor {
			return nil, invalid(field, "line total overflows")
		}
		line := int64(it.Quantity) * it.UnitPriceMinor
		if total > math.MaxInt64-line {
			return nil, invalid("items", "order total overflows")
		}
		total += line
		copied[i] = it
	}

	now = now.UTC().Truncate(time.Microsecond) // Postgres stores microseconds
	return &Order{
		ID: id, CustomerID: customerID, Items: copied, TotalMinor: total,
		Currency: currency, Status: StatusPending, IdempotencyKey: idempotencyKey,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func validCurrency(c string) bool {
	if len(c) != 3 {
		return false
	}
	for _, r := range c {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func (o *Order) transition(next Status, now time.Time) error {
	if !o.Status.canBecome(next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, o.Status, next)
	}
	o.Status = next
	o.Version++
	o.UpdatedAt = now.UTC().Truncate(time.Microsecond)
	return nil
}

// MarkPaid records a successful payment.
func (o *Order) MarkPaid(now time.Time) error { return o.transition(StatusPaid, now) }

// MarkPaymentFailed records a declined payment.
func (o *Order) MarkPaymentFailed(reason string, now time.Time) error {
	if err := o.transition(StatusPaymentFailed, now); err != nil {
		return err
	}
	o.FailureReason = reason
	return nil
}

// Cancel cancels a PENDING or PAID order.
func (o *Order) Cancel(reason string, now time.Time) error {
	if len(reason) > 512 {
		return invalid("reason", "must be at most 512 characters")
	}
	if err := o.transition(StatusCancelled, now); err != nil {
		return err
	}
	o.FailureReason = reason
	return nil
}

// Clone returns a deep copy (used by caches so callers can't mutate shared state).
func (o *Order) Clone() *Order {
	c := *o
	c.Items = append([]Item(nil), o.Items...)
	return &c
}
