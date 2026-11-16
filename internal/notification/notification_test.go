package notification

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
)

// recordingSender remembers what was sent, in order.
type recordingSender struct {
	mu   sync.Mutex
	sent []Notification
	err  error
}

func (r *recordingSender) Send(_ context.Context, n Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.sent = append(r.sent, n)
	return nil
}

func (r *recordingSender) subjects() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.sent))
	for i, n := range r.sent {
		out[i] = n.Subject
	}
	return out
}

// gateLimiter blocks every Wait until the test releases it, which lets a test
// build up a queue deterministically (no sleeps, no flakiness).
type gateLimiter struct {
	gate    chan struct{}
	waiting *atomic.Int32 // how many callers are parked in Wait
}

func (g gateLimiter) Wait(ctx context.Context) error {
	g.waiting.Add(1)
	defer g.waiting.Add(-1)
	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type noLimit struct{}

func (noLimit) Wait(context.Context) error { return nil }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSchedulerSendsHighPriorityFirst(t *testing.T) {
	sender := &recordingSender{}
	limiter := gateLimiter{gate: make(chan struct{}), waiting: &atomic.Int32{}}
	s := NewScheduler(sender, limiter)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx, 1) }()

	var wg sync.WaitGroup
	submit := func(subject string, p Priority) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Submit(ctx, Notification{Subject: subject, Priority: p}); err != nil {
				t.Errorf("submit %s: %v", subject, err)
			}
		}()
	}

	// "first" is popped immediately and parks at the rate limiter...
	submit("first", PriorityLow)
	waitFor(t, func() bool { return limiter.waiting.Load() == 1 })
	// ...while these three queue up behind it.
	submit("low", PriorityLow)
	waitFor(t, func() bool { return s.Pending() == 1 })
	submit("normal", PriorityNormal)
	waitFor(t, func() bool { return s.Pending() == 2 })
	submit("high", PriorityHigh)
	waitFor(t, func() bool { return s.Pending() == 3 })

	close(limiter.gate) // open the floodgates
	wg.Wait()

	got := sender.subjects()
	want := []string{"first", "high", "normal", "low"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("send order = %v, want %v", got, want)
		}
	}
}

func TestSchedulerReturnsSendError(t *testing.T) {
	boom := errors.New("smtp down")
	s := NewScheduler(&recordingSender{err: boom}, noLimit{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Run(ctx, 2) }()
	if err := s.Submit(ctx, Notification{Subject: "x"}); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
}

func TestSchedulerFailsQueuedJobsOnShutdown(t *testing.T) {
	limiter := gateLimiter{gate: make(chan struct{}), waiting: &atomic.Int32{}} // never opens
	s := NewScheduler(&recordingSender{}, limiter)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx, 1); close(done) }()

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errs <- s.Submit(context.Background(), Notification{Subject: "x"}) }()
	}
	waitFor(t, func() bool { return s.Pending() == 1 })
	cancel()
	<-done
	for i := 0; i < 2; i++ {
		if err := <-errs; !errors.Is(err, ErrSchedulerStopped) {
			t.Fatalf("got %v, want ErrSchedulerStopped", err)
		}
	}
}

func newHandler(t *testing.T, sender Sender) *Handler {
	t.Helper()
	s := NewScheduler(sender, noLimit{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = s.Run(ctx, 2) }()
	return NewHandler(s, 100, 3, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func paymentFailed(t *testing.T) kafka.Message {
	t.Helper()
	e := &eventsv1.PaymentFailed{OrderId: "o-1", CustomerId: "c-1", Reason: "insufficient_funds"}
	e.Metadata = events.NewMetadata(context.Background(), "payment-service", e)
	om, err := events.ToOutbox(events.TopicPayments, "o-1", e)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: om.Payload, Headers: om.Headers}
}

func TestHandlerSendsAndDeduplicates(t *testing.T) {
	sender := &recordingSender{}
	h := newHandler(t, sender)
	msg := paymentFailed(t)

	for i := 0; i < 3; i++ { // Kafka redelivery
		if err := h.Handle(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d notifications, want 1", len(sender.sent))
	}
	if n := sender.sent[0]; n.Priority != PriorityHigh || n.CustomerID != "c-1" {
		t.Fatalf("notification = %+v", n)
	}
	if len(h.Recent()) != 1 {
		t.Fatal("history should contain the notification")
	}
}

// A failed send must NOT be marked as seen, or the retry would be skipped.
func TestFailedSendIsRetriedNotDeduplicated(t *testing.T) {
	sender := &recordingSender{err: errors.New("provider 503")}
	h := newHandler(t, sender)
	msg := paymentFailed(t)

	if err := h.Handle(context.Background(), msg); err == nil {
		t.Fatal("expected error")
	}
	sender.mu.Lock()
	sender.err = nil
	sender.mu.Unlock()
	if err := h.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent = %d, want 1", len(sender.sent))
	}
}

func TestMoneyFormatting(t *testing.T) {
	if got := money(123405, "USD"); got != "1234.05 USD" {
		t.Fatalf("got %q", got)
	}
}
