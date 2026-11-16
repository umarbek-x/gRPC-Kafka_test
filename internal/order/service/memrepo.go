package service

import (
	"context"
	"sort"
	"sync"

	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
)

// MemoryRepository is an in-memory Repository. It is a FAKE (a working, simple
// implementation), not a mock: tests exercise real behaviour (idempotency,
// inbox de-duplication, atomic update) without a database. It is exported so
// other packages' tests (gRPC handler tests) can reuse it.
type MemoryRepository struct {
	mu     sync.Mutex
	orders map[string]*domain.Order
	idem   map[string]string // customerID|key -> orderID
	inbox  map[InboxKey]bool
	Outbox []outbox.Message // "published" events, inspectable in tests
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{orders: map[string]*domain.Order{}, idem: map[string]string{}, inbox: map[InboxKey]bool{}}
}

func (r *MemoryRepository) Create(_ context.Context, o *domain.Order, evts []outbox.Message) (*domain.Order, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if o.IdempotencyKey != "" {
		k := o.CustomerID + "|" + o.IdempotencyKey
		if id, ok := r.idem[k]; ok {
			return r.orders[id].Clone(), false, nil
		}
		r.idem[k] = o.ID
	}
	r.orders[o.ID] = o.Clone()
	r.Outbox = append(r.Outbox, evts...)
	return o.Clone(), true, nil
}

func (r *MemoryRepository) Get(_ context.Context, id string) (*domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	o, ok := r.orders[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return o.Clone(), nil
}

func (r *MemoryRepository) List(_ context.Context, customerID string, after *Cursor, limit int) ([]*domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var all []*domain.Order
	for _, o := range r.orders {
		if o.CustomerID == customerID {
			all = append(all, o.Clone())
		}
	}
	sort.Slice(all, func(i, j int) bool { // created_at DESC, id DESC
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt)
		}
		return all[i].ID > all[j].ID
	})
	var out []*domain.Order
	for _, o := range all {
		if after != nil {
			if o.CreatedAt.After(after.CreatedAt) || (o.CreatedAt.Equal(after.CreatedAt) && o.ID >= after.ID) {
				continue
			}
		}
		out = append(out, o)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (r *MemoryRepository) Update(_ context.Context, id string, inbox *InboxKey, fn UpdateFunc) (*domain.Order, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if inbox != nil && r.inbox[*inbox] {
		return nil, domain.ErrAlreadyProcessed
	}
	cur, ok := r.orders[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	work := cur.Clone() // mutate a copy: an error leaves state untouched ("rollback")
	evts, err := fn(work)
	if err != nil {
		return nil, err
	}
	r.orders[id] = work
	if inbox != nil {
		r.inbox[*inbox] = true
	}
	r.Outbox = append(r.Outbox, evts...)
	return work.Clone(), nil
}
