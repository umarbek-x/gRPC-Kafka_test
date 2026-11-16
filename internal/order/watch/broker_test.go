package watch

import (
	"testing"
	"time"

	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
)

func TestFanOutToSubscribersOfSameOrder(t *testing.T) {
	b := NewBroker()
	a1, unsubA1 := b.Subscribe("A")
	a2, unsubA2 := b.Subscribe("A")
	other, unsubOther := b.Subscribe("B")
	defer unsubA1()
	defer unsubA2()
	defer unsubOther()

	b.Publish(&domain.Order{ID: "A", Status: domain.StatusPaid})

	for _, ch := range []<-chan *domain.Order{a1, a2} {
		select {
		case o := <-ch:
			if o.Status != domain.StatusPaid {
				t.Fatalf("got %s", o.Status)
			}
		case <-time.After(time.Second):
			t.Fatal("subscriber did not receive update")
		}
	}
	select {
	case <-other:
		t.Fatal("subscriber of another order must not receive the update")
	default:
	}
}

func TestUnsubscribeCleansUp(t *testing.T) {
	b := NewBroker()
	_, unsub := b.Subscribe("A")
	if b.Subscribers() != 1 {
		t.Fatal("expected 1 subscriber")
	}
	unsub()
	unsub() // idempotent
	if b.Subscribers() != 0 || len(b.subs) != 0 {
		t.Fatal("subscription leaked")
	}
}

func TestPublishNeverBlocksOnSlowSubscriber(t *testing.T) {
	b := NewBroker()
	_, unsub := b.Subscribe("A") // never reads
	defer unsub()

	done := make(chan struct{})
	go func() {
		for i := 0; i < subscriberBuffer*10; i++ {
			b.Publish(&domain.Order{ID: "A"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a slow subscriber")
	}
}
