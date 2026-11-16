package eventhandler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

// stubOrders records calls and returns a preset error.
type stubOrders struct {
	completed, failed []string
	err               error
}

func (s *stubOrders) ApplyPaymentCompleted(_ context.Context, _, orderID string) error {
	s.completed = append(s.completed, orderID)
	return s.err
}

func (s *stubOrders) ApplyPaymentFailed(_ context.Context, _, orderID, _ string) error {
	s.failed = append(s.failed, orderID)
	return s.err
}

func message(t *testing.T, evt events.Event) kafka.Message {
	t.Helper()
	om, err := events.ToOutbox(events.TopicPayments, "o-1", evt)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: om.Topic, Key: om.Key, Value: om.Payload, Headers: om.Headers}
}

func completed(t *testing.T) kafka.Message {
	e := &eventsv1.PaymentCompleted{OrderId: "o-1", PaymentId: "p-1"}
	e.Metadata = events.NewMetadata(context.Background(), "payment-service", e)
	return message(t, e)
}

func TestRoutesByEventType(t *testing.T) {
	stub := &stubOrders{}
	h := New(stub, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := h.Handle(context.Background(), completed(t)); err != nil {
		t.Fatal(err)
	}
	failed := &eventsv1.PaymentFailed{OrderId: "o-2", Reason: "declined"}
	failed.Metadata = events.NewMetadata(context.Background(), "payment-service", failed)
	if err := h.Handle(context.Background(), message(t, failed)); err != nil {
		t.Fatal(err)
	}
	refunded := &eventsv1.PaymentRefunded{OrderId: "o-3"}
	refunded.Metadata = events.NewMetadata(context.Background(), "payment-service", refunded)
	if err := h.Handle(context.Background(), message(t, refunded)); err != nil {
		t.Fatalf("unknown-to-us event types must be ignored, got %v", err)
	}

	if len(stub.completed) != 1 || stub.completed[0] != "o-1" || len(stub.failed) != 1 || stub.failed[0] != "o-2" {
		t.Fatalf("routing wrong: %+v", stub)
	}
}

// The error classification decides between "ack", "retry" and "DLQ".
func TestErrorClassification(t *testing.T) {
	transient := errors.New("connection refused")
	tests := []struct {
		name          string
		serviceErr    error
		wantNil       bool
		wantPermanent bool
	}{
		{"success", nil, true, false},
		{"duplicate is acked", domain.ErrAlreadyProcessed, true, false},
		{"invalid transition is acked", domain.ErrInvalidTransition, true, false},
		{"not found goes to DLQ", domain.ErrNotFound, false, true},
		{"transient is retried", transient, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(&stubOrders{err: tt.serviceErr}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			err := h.Handle(context.Background(), completed(t))
			if (err == nil) != tt.wantNil {
				t.Fatalf("err = %v, wantNil = %v", err, tt.wantNil)
			}
			if retry.IsPermanent(err) != tt.wantPermanent {
				t.Fatalf("permanent = %v, want %v", retry.IsPermanent(err), tt.wantPermanent)
			}
		})
	}
}

func TestMalformedMessageIsPermanent(t *testing.T) {
	h := New(&stubOrders{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := h.Handle(context.Background(), kafka.Message{Value: []byte("not json"),
		Headers: map[string]string{events.HeaderEventType: "orderflow.events.v1.PaymentCompleted"}})
	if !retry.IsPermanent(err) {
		t.Fatalf("want permanent error, got %v", err)
	}
}
