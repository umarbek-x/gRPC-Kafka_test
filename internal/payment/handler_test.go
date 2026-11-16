package payment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/google/uuid"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

// memStore is an in-memory fake of Store with the same semantics as Postgres.
type memStore struct {
	mu       sync.Mutex
	payments map[string]*Payment // by order ID
	inbox    map[string]bool
	outbox   []outbox.Message
	failNext error // inject one failure into Record
}

func newMemStore() *memStore {
	return &memStore{payments: map[string]*Payment{}, inbox: map[string]bool{}}
}

func (s *memStore) GetByOrder(_ context.Context, orderID string) (*Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.payments[orderID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *p
	return &cp, nil
}

func (s *memStore) Record(_ context.Context, eventID string, p *Payment, evts []outbox.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext != nil {
		err := s.failNext
		s.failNext = nil
		return err
	}
	if s.inbox[eventID] {
		return ErrAlreadyProcessed
	}
	if _, exists := s.payments[p.OrderID]; exists {
		return ErrAlreadyProcessed
	}
	s.inbox[eventID] = true
	cp := *p
	s.payments[p.OrderID] = &cp
	s.outbox = append(s.outbox, evts...)
	return nil
}

func (s *memStore) MarkRefunded(_ context.Context, eventID, orderID string, build func(*Payment) ([]outbox.Message, error)) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inbox[eventID] {
		return false, ErrAlreadyProcessed
	}
	s.inbox[eventID] = true
	p, ok := s.payments[orderID]
	if !ok || p.Status != StatusCompleted {
		return false, nil
	}
	evts, err := build(p)
	if err != nil {
		return false, err
	}
	p.Status = StatusRefunded
	s.outbox = append(s.outbox, evts...)
	return true, nil
}

// countingGateway wraps FakeGateway and counts real calls.
type countingGateway struct {
	*FakeGateway
	mu              sync.Mutex
	charges         int
	refunds         int
	failChargeTimes int
}

func (g *countingGateway) Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error) {
	g.mu.Lock()
	g.charges++
	fail := g.failChargeTimes > 0
	if fail {
		g.failChargeTimes--
	}
	g.mu.Unlock()
	if fail {
		return ChargeResult{}, ErrGatewayUnavailable
	}
	return g.FakeGateway.Charge(ctx, req)
}

func (g *countingGateway) Refund(ctx context.Context, id string) error {
	g.mu.Lock()
	g.refunds++
	g.mu.Unlock()
	return g.FakeGateway.Refund(ctx, id)
}

func setup() (*Handler, *memStore, *countingGateway) {
	store := newMemStore()
	gw := &countingGateway{FakeGateway: NewFakeGateway(0, 0, 100_000)}
	return NewHandler(store, gw, slog.New(slog.NewTextHandler(io.Discard, nil))), store, gw
}

func toMessage(t *testing.T, key string, evt events.Event) kafka.Message {
	t.Helper()
	om, err := events.ToOutbox(events.TopicOrders, key, evt)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: om.Topic, Key: om.Key, Value: om.Payload, Headers: om.Headers}
}

func orderCreated(t *testing.T, orderID string, total int64) kafka.Message {
	e := &eventsv1.OrderCreated{OrderId: orderID, CustomerId: "cust-1", TotalMinor: total, Currency: "USD"}
	e.Metadata = events.NewMetadata(context.Background(), "order-service", e)
	return toMessage(t, orderID, e)
}

func orderCancelled(t *testing.T, orderID string) kafka.Message {
	e := &eventsv1.OrderCancelled{OrderId: orderID, CustomerId: "cust-1", Reason: "changed mind"}
	e.Metadata = events.NewMetadata(context.Background(), "order-service", e)
	return toMessage(t, orderID, e)
}

func lastEventType(s *memStore) string {
	if len(s.outbox) == 0 {
		return ""
	}
	return s.outbox[len(s.outbox)-1].EventType
}

func TestApprovedPaymentEmitsCompleted(t *testing.T) {
	h, store, _ := setup()
	orderID := uuid.NewString()
	if err := h.Handle(context.Background(), orderCreated(t, orderID, 5_000)); err != nil {
		t.Fatal(err)
	}
	p, _ := store.GetByOrder(context.Background(), orderID)
	if p.Status != StatusCompleted || p.AmountMinor != 5_000 {
		t.Fatalf("payment = %+v", p)
	}
	if lastEventType(store) != "orderflow.events.v1.PaymentCompleted" {
		t.Fatalf("event = %s", lastEventType(store))
	}
	if store.outbox[0].Key != orderID || store.outbox[0].Topic != events.TopicPayments {
		t.Fatal("event must be keyed by order ID on payments.v1")
	}
}

func TestDeclinedPaymentEmitsFailed(t *testing.T) {
	h, store, _ := setup()
	orderID := uuid.NewString()
	if err := h.Handle(context.Background(), orderCreated(t, orderID, 100_001)); err != nil {
		t.Fatalf("a decline is a business outcome, not an error: %v", err)
	}
	p, _ := store.GetByOrder(context.Background(), orderID)
	if p.Status != StatusFailed || p.FailureReason != "insufficient_funds" {
		t.Fatalf("payment = %+v", p)
	}
	if lastEventType(store) != "orderflow.events.v1.PaymentFailed" {
		t.Fatalf("event = %s", lastEventType(store))
	}
}

// At-least-once delivery: the same Kafka message arrives twice.
func TestRedeliveryDoesNotChargeTwice(t *testing.T) {
	h, store, gw := setup()
	msg := orderCreated(t, uuid.NewString(), 5_000)
	for i := 0; i < 3; i++ {
		if err := h.Handle(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	if gw.charges != 1 || len(store.outbox) != 1 {
		t.Fatalf("charges=%d events=%d, want 1 and 1", gw.charges, len(store.outbox))
	}
}

// Crash window: the gateway charged, but saving the result failed. On retry
// the gateway is called again with the same idempotency key and must return
// the stored result instead of charging again.
func TestRetryAfterDBFailureIsSafe(t *testing.T) {
	h, store, _ := setup()
	orderID := uuid.NewString()
	msg := orderCreated(t, orderID, 5_000)

	store.failNext = errors.New("connection reset")
	if err := h.Handle(context.Background(), msg); err == nil || retry.IsPermanent(err) {
		t.Fatalf("want a transient error, got %v", err)
	}
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(store.outbox) != 1 {
		t.Fatalf("events = %d, want exactly 1", len(store.outbox))
	}
}

func TestGatewayOutageIsTransient(t *testing.T) {
	h, store, gw := setup()
	gw.failChargeTimes = 1
	msg := orderCreated(t, uuid.NewString(), 5_000)
	err := h.Handle(context.Background(), msg)
	if !errors.Is(err, ErrGatewayUnavailable) || retry.IsPermanent(err) {
		t.Fatalf("want retryable gateway error, got %v", err)
	}
	if len(store.outbox) != 0 {
		t.Fatal("no event may be emitted for an unknown outcome")
	}
}

func TestCancelRefundsCompletedPayment(t *testing.T) {
	h, store, gw := setup()
	orderID := uuid.NewString()
	ctx := context.Background()
	_ = h.Handle(ctx, orderCreated(t, orderID, 5_000))

	cancel := orderCancelled(t, orderID)
	for i := 0; i < 2; i++ { // delivered twice
		if err := h.Handle(ctx, cancel); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := store.GetByOrder(ctx, orderID)
	if p.Status != StatusRefunded || gw.refunds != 1 {
		t.Fatalf("status=%s refunds=%d", p.Status, gw.refunds)
	}
	if lastEventType(store) != "orderflow.events.v1.PaymentRefunded" || len(store.outbox) != 2 {
		t.Fatalf("outbox = %d, last = %s", len(store.outbox), lastEventType(store))
	}
}

func TestCancelWithoutPaymentIsNoop(t *testing.T) {
	h, store, gw := setup()
	if err := h.Handle(context.Background(), orderCancelled(t, uuid.NewString())); err != nil {
		t.Fatal(err)
	}
	if gw.refunds != 0 || len(store.outbox) != 0 {
		t.Fatal("nothing to refund")
	}
}

func TestPoisonMessageIsPermanent(t *testing.T) {
	h, _, _ := setup()
	err := h.Handle(context.Background(), kafka.Message{Value: []byte("{"), Headers: map[string]string{events.HeaderEventType: "orderflow.events.v1.OrderCreated"}})
	if !retry.IsPermanent(err) {
		t.Fatalf("want permanent, got %v", err)
	}
}
