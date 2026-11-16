package notification

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/ds/lru"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/ds/ringbuffer"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

const ServiceName = "notification-service"

// Handler consumes orders.v1 and payments.v1.
type Handler struct {
	scheduler *Scheduler
	// seen de-duplicates redelivered events. It is BEST EFFORT: it is lost on
	// restart and not shared between replicas, so a duplicate email is
	// possible after a crash. For notifications that trade-off is normally
	// acceptable; where it isn't (payments!), use a durable inbox table as
	// the other two services do.
	seen   *lru.Cache[string, struct{}]
	recent *ringbuffer.Buffer[Notification]
	log    *slog.Logger
}

func NewHandler(s *Scheduler, dedupSize, historySize int, log *slog.Logger) *Handler {
	return &Handler{
		scheduler: s,
		seen:      lru.New[string, struct{}](dedupSize, 24*time.Hour),
		recent:    ringbuffer.New[Notification](historySize),
		log:       log,
	}
}

// Recent returns the last sent notifications, oldest first.
func (h *Handler) Recent() []Notification { return h.recent.Snapshot() }

// Handle implements kafka.Handler.
func (h *Handler) Handle(ctx context.Context, msg kafka.Message) error {
	ctx = events.ContextFrom(ctx, msg)
	evt, err := events.Decode(msg)
	if err != nil {
		return retry.Permanent(err)
	}
	n, ok := build(evt)
	if !ok {
		return nil // an event type we don't notify about
	}
	if h.seen.Contains(n.EventID) {
		h.log.InfoContext(ctx, "duplicate event ignored", "event_id", n.EventID)
		return nil
	}

	if err := h.scheduler.Submit(ctx, n); err != nil {
		return err // retried by the consumer
	}
	// Mark as seen only AFTER a successful send, otherwise a failed send
	// would be "de-duplicated" away on retry and never delivered.
	h.seen.Put(n.EventID, struct{}{})
	n.SentAt = time.Now().UTC()
	h.recent.Push(n)
	return nil
}

func money(minor int64, currency string) string {
	return fmt.Sprintf("%d.%02d %s", minor/100, minor%100, currency)
}

// build maps an event to a notification.
func build(evt events.Event) (Notification, bool) {
	id := evt.GetMetadata().GetEventId()
	switch e := evt.(type) {
	case *eventsv1.OrderCreated:
		return Notification{EventID: id, CustomerID: e.GetCustomerId(), OrderID: e.GetOrderId(), Priority: PriorityLow,
			Subject: "We received your order",
			Body:    fmt.Sprintf("Order %s for %s is being processed.", e.GetOrderId(), money(e.GetTotalMinor(), e.GetCurrency()))}, true
	case *eventsv1.OrderCancelled:
		return Notification{EventID: id, CustomerID: e.GetCustomerId(), OrderID: e.GetOrderId(), Priority: PriorityNormal,
			Subject: "Your order was cancelled",
			Body:    fmt.Sprintf("Order %s was cancelled. Reason: %s", e.GetOrderId(), e.GetReason())}, true
	case *eventsv1.PaymentCompleted:
		return Notification{EventID: id, CustomerID: e.GetCustomerId(), OrderID: e.GetOrderId(), Priority: PriorityNormal,
			Subject: "Payment confirmed",
			Body:    fmt.Sprintf("We charged %s for order %s.", money(e.GetAmountMinor(), e.GetCurrency()), e.GetOrderId())}, true
	case *eventsv1.PaymentFailed:
		return Notification{EventID: id, CustomerID: e.GetCustomerId(), OrderID: e.GetOrderId(), Priority: PriorityHigh,
			Subject: "Action needed: payment failed",
			Body:    fmt.Sprintf("Payment for order %s failed: %s.", e.GetOrderId(), e.GetReason())}, true
	case *eventsv1.PaymentRefunded:
		return Notification{EventID: id, CustomerID: e.GetCustomerId(), OrderID: e.GetOrderId(), Priority: PriorityNormal,
			Subject: "Refund issued",
			Body:    fmt.Sprintf("We refunded %s for order %s.", money(e.GetAmountMinor(), e.GetCurrency()), e.GetOrderId())}, true
	}
	return Notification{}, false
}

// LogSender "sends" by writing a log line. Swap it for SES/SendGrid/Twilio.
type LogSender struct {
	Log     *slog.Logger
	Latency time.Duration
}

func (s LogSender) Send(ctx context.Context, n Notification) error {
	if s.Latency > 0 {
		t := time.NewTimer(s.Latency)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	s.Log.InfoContext(ctx, "notification sent", "to", n.CustomerID, "order_id", n.OrderID, "subject", n.Subject, "priority", int(n.Priority))
	return nil
}
