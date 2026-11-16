// Package eventhandler is the Kafka ADAPTER of the order-service: it turns
// payment events into service calls.
package eventhandler

import (
	"context"
	"errors"
	"log/slog"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

// Orders is the small part of the service this handler needs. Declaring the
// interface at the consumer side keeps the handler easy to test.
type Orders interface {
	ApplyPaymentCompleted(ctx context.Context, eventID, orderID string) error
	ApplyPaymentFailed(ctx context.Context, eventID, orderID, reason string) error
}

type Handler struct {
	orders Orders
	log    *slog.Logger
}

func New(orders Orders, log *slog.Logger) *Handler {
	return &Handler{orders: orders, log: log}
}

// Handle implements kafka.Handler for topic payments.v1.
func (h *Handler) Handle(ctx context.Context, msg kafka.Message) error {
	ctx = events.ContextFrom(ctx, msg)

	evt, err := events.Decode(msg)
	if err != nil {
		// Malformed message: retrying can't fix it. Permanent => DLQ.
		return retry.Permanent(err)
	}
	eventID := evt.GetMetadata().GetEventId()

	switch e := evt.(type) {
	case *eventsv1.PaymentCompleted:
		err = h.orders.ApplyPaymentCompleted(ctx, eventID, e.GetOrderId())
	case *eventsv1.PaymentFailed:
		err = h.orders.ApplyPaymentFailed(ctx, eventID, e.GetOrderId(), e.GetReason())
	default:
		// Other event types on this topic (e.g. PaymentRefunded) are not our
		// business. Ignoring unknown types lets producers add new ones freely.
		return nil
	}

	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrAlreadyProcessed):
		h.log.InfoContext(ctx, "duplicate event ignored", "event_id", eventID)
		return nil
	case errors.Is(err, domain.ErrInvalidTransition):
		// e.g. payment completed for an order the customer already cancelled.
		// A valid business situation, not a failure: the payment-service
		// refunds when it sees OrderCancelled. Don't retry, don't DLQ.
		h.log.WarnContext(ctx, "event not applicable to current order state", "event_id", eventID, "err", err)
		return nil
	case errors.Is(err, domain.ErrNotFound):
		return retry.Permanent(err)
	default:
		return err // transient (DB down, timeout): retry with backoff
	}
}
