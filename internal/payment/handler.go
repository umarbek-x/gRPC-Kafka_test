package payment

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

// Handler consumes orders.v1.
type Handler struct {
	store   Store
	gateway Gateway
	log     *slog.Logger
	now     func() time.Time
}

func NewHandler(store Store, gateway Gateway, log *slog.Logger) *Handler {
	return &Handler{store: store, gateway: gateway, log: log, now: time.Now}
}

// Handle implements kafka.Handler.
func (h *Handler) Handle(ctx context.Context, msg kafka.Message) error {
	ctx = events.ContextFrom(ctx, msg)
	evt, err := events.Decode(msg)
	if err != nil {
		return retry.Permanent(err)
	}
	switch e := evt.(type) {
	case *eventsv1.OrderCreated:
		return h.onOrderCreated(ctx, e)
	case *eventsv1.OrderCancelled:
		return h.onOrderCancelled(ctx, e)
	default:
		return nil
	}
}

// onOrderCreated charges the customer exactly once, even though the event may
// be delivered many times and the process may crash at any line.
//
// Three layers of protection, from cheap to strong:
//  1. fast path: a payment row for this order already exists -> done
//  2. gateway idempotency key = order ID -> a repeated Charge never
//     double-charges, it returns the first result
//  3. inbox + UNIQUE(order_id) inside the DB transaction -> the result is
//     recorded and the outgoing event emitted exactly once
//
// Important rule: the gateway call happens OUTSIDE the database transaction.
// Never hold a DB transaction (and its connection + row locks) open across a
// slow network call.
func (h *Handler) onOrderCreated(ctx context.Context, e *eventsv1.OrderCreated) error {
	eventID := e.GetMetadata().GetEventId()
	orderID := e.GetOrderId()
	if _, err := uuid.Parse(orderID); err != nil {
		return retry.Permanent(errors.New("order_id is not a UUID"))
	}

	if _, err := h.store.GetByOrder(ctx, orderID); err == nil {
		h.log.InfoContext(ctx, "payment already exists, skipping", "order_id", orderID)
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	res, err := h.gateway.Charge(ctx, ChargeRequest{IdempotencyKey: orderID, AmountMinor: e.GetTotalMinor(), Currency: e.GetCurrency()})
	if err != nil {
		return err // unknown outcome: retry (safe thanks to the idempotency key)
	}

	now := h.now().UTC().Truncate(time.Microsecond)
	p := &Payment{
		ID: uuid.NewString(), OrderID: orderID, CustomerID: e.GetCustomerId(),
		AmountMinor: e.GetTotalMinor(), Currency: e.GetCurrency(),
		Status: StatusCompleted, CreatedAt: now, UpdatedAt: now,
	}
	var out events.Event
	if res.Approved {
		ev := &eventsv1.PaymentCompleted{PaymentId: p.ID, OrderId: orderID, CustomerId: p.CustomerID, AmountMinor: p.AmountMinor, Currency: p.Currency}
		ev.Metadata = events.NewMetadata(ctx, ServiceName, ev)
		out = ev
	} else {
		p.Status, p.FailureReason = StatusFailed, res.DeclineReason
		ev := &eventsv1.PaymentFailed{PaymentId: p.ID, OrderId: orderID, CustomerId: p.CustomerID, Reason: res.DeclineReason}
		ev.Metadata = events.NewMetadata(ctx, ServiceName, ev)
		out = ev
	}
	om, err := events.ToOutbox(events.TopicPayments, orderID, out)
	if err != nil {
		return retry.Permanent(err)
	}

	err = h.store.Record(ctx, eventID, p, []outbox.Message{om})
	if errors.Is(err, ErrAlreadyProcessed) {
		h.log.InfoContext(ctx, "duplicate event ignored", "event_id", eventID)
		return nil
	}
	if err != nil {
		return err
	}
	h.log.InfoContext(ctx, "payment processed", "order_id", orderID, "payment_id", p.ID, "status", string(p.Status), "amount_minor", p.AmountMinor)
	return nil
}

// onOrderCancelled refunds a captured payment (a "compensating action": in an
// event-driven system you cannot roll back another service's commit, you
// issue a new action that undoes its effect. This is the heart of the Saga
// pattern).
func (h *Handler) onOrderCancelled(ctx context.Context, e *eventsv1.OrderCancelled) error {
	eventID := e.GetMetadata().GetEventId()
	orderID := e.GetOrderId()
	if _, err := uuid.Parse(orderID); err != nil {
		return retry.Permanent(errors.New("order_id is not a UUID"))
	}

	p, err := h.store.GetByOrder(ctx, orderID)
	if errors.Is(err, ErrNotFound) {
		return nil // nothing was charged
	}
	if err != nil {
		return err
	}
	if p.Status != StatusCompleted {
		return nil // failed or already refunded: nothing to give back
	}

	if err := h.gateway.Refund(ctx, p.ID); err != nil {
		return err
	}
	refunded, err := h.store.MarkRefunded(ctx, eventID, orderID, func(p *Payment) ([]outbox.Message, error) {
		ev := &eventsv1.PaymentRefunded{PaymentId: p.ID, OrderId: orderID, CustomerId: p.CustomerID, AmountMinor: p.AmountMinor, Currency: p.Currency}
		ev.Metadata = events.NewMetadata(ctx, ServiceName, ev)
		om, err := events.ToOutbox(events.TopicPayments, orderID, ev)
		if err != nil {
			return nil, err
		}
		return []outbox.Message{om}, nil
	})
	if errors.Is(err, ErrAlreadyProcessed) {
		return nil
	}
	if err != nil {
		return err
	}
	if refunded {
		h.log.InfoContext(ctx, "payment refunded", "order_id", orderID, "payment_id", p.ID)
	}
	return nil
}
