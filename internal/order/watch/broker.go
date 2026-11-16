// Package watch is an in-process publish/subscribe broker used by the
// WatchOrder streaming RPC.
//
// Each subscriber gets its own buffered channel. Publish fans an update out
// to every subscriber of that order.
//
// Slow-consumer policy: Publish NEVER blocks. If a subscriber's buffer is
// full the update is dropped for that subscriber. One stuck client must not
// be able to stall the goroutine that publishes (the Kafka consumer).
//
// Limitation worth knowing: this broker lives in one process. With several
// order-service replicas, a client connected to replica A does not see an
// update handled by replica B. Production fixes: Redis pub/sub, or giving
// each replica its own consumer group so every replica sees every event.
package watch

import (
	"sync"

	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
)

const subscriberBuffer = 8

type Broker struct {
	mu     sync.RWMutex
	nextID uint64
	subs   map[string]map[uint64]chan *domain.Order // orderID -> subscriberID -> channel
}

func NewBroker() *Broker {
	return &Broker{subs: make(map[string]map[uint64]chan *domain.Order)}
}

// Subscribe returns a channel of updates for one order and an unsubscribe
// function that MUST be called (use defer) to avoid leaking the subscription.
func (b *Broker) Subscribe(orderID string) (<-chan *domain.Order, func()) {
	ch := make(chan *domain.Order, subscriberBuffer)

	b.mu.Lock()
	b.nextID++
	id := b.nextID
	if b.subs[orderID] == nil {
		b.subs[orderID] = make(map[uint64]chan *domain.Order)
	}
	b.subs[orderID][id] = ch
	b.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			delete(b.subs[orderID], id)
			if len(b.subs[orderID]) == 0 {
				delete(b.subs, orderID) // don't let the outer map grow forever
			}
		})
	}
}

// Publish delivers an update to all current subscribers of the order.
func (b *Broker) Publish(o *domain.Order) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs[o.ID] {
		select {
		case ch <- o.Clone():
		default: // buffer full: drop for this subscriber
		}
	}
}

// Subscribers returns the number of active subscriptions (for metrics/tests).
func (b *Broker) Subscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, m := range b.subs {
		n += len(m)
	}
	return n
}
