package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/watch"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
)

func newTestService() (*Service, *MemoryRepository) {
	repo := NewMemoryRepository()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := New(repo, watch.NewBroker(), 100, time.Minute, log)
	// Deterministic, strictly increasing clock so ordering is predictable.
	tick := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { tick = tick.Add(time.Second); return tick }
	return svc, repo
}

func input(key string) CreateInput {
	return CreateInput{CustomerID: "cust-1", Currency: "USD", IdempotencyKey: key,
		Items: []domain.Item{{ProductID: "sku-1", Quantity: 2, UnitPriceMinor: 500}}}
}

func TestCreateOrderWritesOutboxEvent(t *testing.T) {
	svc, repo := newTestService()
	o, created, err := svc.CreateOrder(context.Background(), input("k1"))
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if len(repo.Outbox) != 1 {
		t.Fatalf("outbox has %d messages, want 1", len(repo.Outbox))
	}
	m := repo.Outbox[0]
	if m.Topic != events.TopicOrders || m.Key != o.ID {
		t.Fatalf("topic/key = %s/%s", m.Topic, m.Key)
	}
	evt, err := events.Decode(kafka.Message{Value: m.Payload, Headers: m.Headers})
	if err != nil {
		t.Fatal(err)
	}
	if oc := evt.(*eventsv1.OrderCreated); oc.OrderId != o.ID || oc.TotalMinor != 1000 {
		t.Fatalf("event mismatch: %+v", oc)
	}
}

func TestCreateOrderIsIdempotent(t *testing.T) {
	svc, repo := newTestService()
	first, _, _ := svc.CreateOrder(context.Background(), input("same-key"))
	second, created, err := svc.CreateOrder(context.Background(), input("same-key"))
	if err != nil {
		t.Fatal(err)
	}
	if created || second.ID != first.ID {
		t.Fatalf("retry must return the original order (created=%v, %s vs %s)", created, second.ID, first.ID)
	}
	if len(repo.Outbox) != 1 {
		t.Fatalf("retry must not emit a second event, outbox=%d", len(repo.Outbox))
	}
}

func TestCreateOrderRejectsInvalidInput(t *testing.T) {
	svc, repo := newTestService()
	in := input("k")
	in.Items = nil
	_, _, err := svc.CreateOrder(context.Background(), in)
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want ValidationError, got %v", err)
	}
	if len(repo.Outbox) != 0 {
		t.Fatal("nothing may be written for invalid input")
	}
}

func TestPaymentCompletedIsIdempotent(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	o, _, _ := svc.CreateOrder(ctx, input("k"))

	if err := svc.ApplyPaymentCompleted(ctx, "evt-1", o.ID); err != nil {
		t.Fatal(err)
	}
	// Kafka redelivers the same event:
	err := svc.ApplyPaymentCompleted(ctx, "evt-1", o.ID)
	if !errors.Is(err, domain.ErrAlreadyProcessed) {
		t.Fatalf("want ErrAlreadyProcessed, got %v", err)
	}
	got, _ := svc.GetOrder(ctx, o.ID)
	if got.Status != domain.StatusPaid || got.Version != 2 {
		t.Fatalf("status=%s version=%d", got.Status, got.Version)
	}
}

func TestCancelEmitsEventAndBlocksLaterPayment(t *testing.T) {
	svc, repo := newTestService()
	ctx := context.Background()
	o, _, _ := svc.CreateOrder(ctx, input("k"))

	cancelled, err := svc.CancelOrder(ctx, o.ID, "changed my mind")
	if err != nil || cancelled.Status != domain.StatusCancelled {
		t.Fatalf("cancel: %v %+v", err, cancelled)
	}
	if last := repo.Outbox[len(repo.Outbox)-1]; last.EventType != "orderflow.events.v1.OrderCancelled" {
		t.Fatalf("last event = %s", last.EventType)
	}
	// A payment result that arrives late must not resurrect the order.
	if err := svc.ApplyPaymentCompleted(ctx, "evt-late", o.ID); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("want ErrInvalidTransition, got %v", err)
	}
	if _, err := svc.CancelOrder(ctx, o.ID, "again"); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("double cancel: want ErrInvalidTransition, got %v", err)
	}
}

func TestGetOrderErrors(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	var ve *domain.ValidationError
	if _, err := svc.GetOrder(ctx, "not-a-uuid"); !errors.As(err, &ve) {
		t.Fatalf("want ValidationError, got %v", err)
	}
	if _, err := svc.GetOrder(ctx, "6f1e6f0e-58a1-4c0b-9d57-0a4f5e3a9b11"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestGetOrderServesFromCacheAndSeesUpdates(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	o, _, _ := svc.CreateOrder(ctx, input("k"))

	before := svc.cache.Stats().Hits
	if _, err := svc.GetOrder(ctx, o.ID); err != nil {
		t.Fatal(err)
	}
	if svc.cache.Stats().Hits != before+1 {
		t.Fatal("expected a cache hit")
	}
	_ = svc.ApplyPaymentFailed(ctx, "evt-1", o.ID, "card declined")
	got, _ := svc.GetOrder(ctx, o.ID)
	if got.Status != domain.StatusPaymentFailed || got.FailureReason != "card declined" {
		t.Fatalf("cache returned stale order: %+v", got)
	}
}

func TestListOrdersPaginates(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	want := map[string]bool{}
	for i := 0; i < 7; i++ {
		o, _, err := svc.CreateOrder(ctx, input(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		want[o.ID] = true
	}

	seen := map[string]bool{}
	token, pages := "", 0
	var prev time.Time
	for {
		page, next, err := svc.ListOrders(ctx, "cust-1", 3, token)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, o := range page {
			if seen[o.ID] {
				t.Fatalf("order %s returned twice", o.ID)
			}
			if !prev.IsZero() && o.CreatedAt.After(prev) {
				t.Fatal("orders must be newest first")
			}
			prev = o.CreatedAt
			seen[o.ID] = true
		}
		if next == "" {
			break
		}
		token = next
	}
	if len(seen) != len(want) || pages != 3 {
		t.Fatalf("saw %d orders in %d pages, want 7 in 3", len(seen), pages)
	}

	var ve *domain.ValidationError
	if _, _, err := svc.ListOrders(ctx, "cust-1", 3, "garbage!!"); !errors.As(err, &ve) {
		t.Fatalf("bad token: want ValidationError, got %v", err)
	}
}

func TestWatchReceivesStatusChange(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	o, _, _ := svc.CreateOrder(ctx, input("k"))

	updates, unsubscribe := svc.Watch(o.ID)
	defer unsubscribe()
	if err := svc.ApplyPaymentCompleted(ctx, "evt-1", o.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case u := <-updates:
		if u.Status != domain.StatusPaid {
			t.Fatalf("got %s", u.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("no update received")
	}
}
