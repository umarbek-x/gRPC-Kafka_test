// Package service contains the order use cases (application layer).
//
// It orchestrates: validate input → call the domain → persist through the
// Repository interface → publish to in-process watchers. It knows nothing
// about gRPC or SQL, so it is tested with an in-memory fake repository.
package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/watch"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/ds/lru"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
)

// ServiceName is used as event producer and Kafka consumer group.
const ServiceName = "order-service"

// Cursor marks a position in the (created_at DESC, id DESC) ordering.
type Cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        string    `json:"id"`
}

// InboxKey identifies a consumed event for de-duplication.
type InboxKey struct {
	Consumer string
	EventID  string
}

// UpdateFunc mutates the order inside a transaction and returns the events
// to publish atomically with the change.
type UpdateFunc func(o *domain.Order) ([]outbox.Message, error)

// Repository is the persistence PORT. The service owns the interface; the
// Postgres adapter implements it ("accept interfaces, return structs").
type Repository interface {
	// Create stores the order + events atomically. If an order with the same
	// (customer, idempotency key) exists, it returns that order and created=false.
	Create(ctx context.Context, o *domain.Order, evts []outbox.Message) (order *domain.Order, created bool, err error)
	Get(ctx context.Context, id string) (*domain.Order, error)
	// List returns up to limit orders after the cursor (nil = first page).
	List(ctx context.Context, customerID string, after *Cursor, limit int) ([]*domain.Order, error)
	// Update loads the order FOR UPDATE, applies fn and saves the result and
	// events in one transaction. If inbox is set and was already recorded, it
	// returns domain.ErrAlreadyProcessed.
	Update(ctx context.Context, id string, inbox *InboxKey, fn UpdateFunc) (*domain.Order, error)
}

type Service struct {
	repo   Repository
	cache  *lru.Cache[string, *domain.Order]
	broker *watch.Broker
	log    *slog.Logger
	now    func() time.Time
	newID  func() string
}

func New(repo Repository, broker *watch.Broker, cacheSize int, cacheTTL time.Duration, log *slog.Logger) *Service {
	return &Service{
		repo:   repo,
		cache:  lru.New[string, *domain.Order](cacheSize, cacheTTL),
		broker: broker,
		log:    log,
		now:    time.Now,
		newID:  uuid.NewString,
	}
}

type CreateInput struct {
	CustomerID     string
	Currency       string
	IdempotencyKey string
	Items          []domain.Item
}

// CreateOrder creates a PENDING order and records an OrderCreated event in
// the outbox in the same transaction.
func (s *Service) CreateOrder(ctx context.Context, in CreateInput) (*domain.Order, bool, error) {
	o, err := domain.NewOrder(s.newID(), in.CustomerID, in.Currency, in.IdempotencyKey, in.Items, s.now())
	if err != nil {
		return nil, false, err
	}

	evt := &eventsv1.OrderCreated{
		OrderId: o.ID, CustomerId: o.CustomerID, TotalMinor: o.TotalMinor, Currency: o.Currency,
	}
	for _, it := range o.Items {
		evt.Items = append(evt.Items, &eventsv1.LineItem{ProductId: it.ProductID, Quantity: it.Quantity, UnitPriceMinor: it.UnitPriceMinor})
	}
	evt.Metadata = events.NewMetadata(ctx, ServiceName, evt)
	// Key = order ID: all events of one order go to one partition => ordered.
	msg, err := events.ToOutbox(events.TopicOrders, o.ID, evt)
	if err != nil {
		return nil, false, err
	}

	saved, created, err := s.repo.Create(ctx, o, []outbox.Message{msg})
	if err != nil {
		return nil, false, err
	}
	if created {
		s.log.InfoContext(ctx, "order created", "order_id", saved.ID, "customer_id", saved.CustomerID, "total_minor", saved.TotalMinor)
	} else {
		s.log.InfoContext(ctx, "idempotent replay of create order", "order_id", saved.ID)
	}
	s.cache.Put(saved.ID, saved.Clone())
	return saved, created, nil
}

// GetOrder uses the cache-aside pattern: try the cache, fall back to the DB,
// then populate the cache.
func (s *Service) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	if o, ok := s.cache.Get(id); ok {
		return o.Clone(), nil
	}
	o, err := s.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	s.cache.Put(id, o.Clone())
	return o, nil
}

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// ListOrders returns one page and the token for the next page ("" if last).
//
// Cursor (keyset) pagination instead of OFFSET: "give me rows after this
// (created_at, id)". It uses the index directly, costs the same for page 1
// and page 10 000, and doesn't skip/duplicate rows when new orders arrive.
func (s *Service) ListOrders(ctx context.Context, customerID string, pageSize int, pageToken string) ([]*domain.Order, string, error) {
	if customerID == "" {
		return nil, "", &domain.ValidationError{Field: "customer_id", Message: "must not be empty"}
	}
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	var after *Cursor
	if pageToken != "" {
		c, err := decodeCursor(pageToken)
		if err != nil {
			return nil, "", &domain.ValidationError{Field: "page_token", Message: "is invalid"}
		}
		after = c
	}
	// Fetch one extra row to learn whether another page exists.
	orders, err := s.repo.List(ctx, customerID, after, pageSize+1)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(orders) > pageSize {
		orders = orders[:pageSize]
		last := orders[len(orders)-1]
		next = encodeCursor(Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	return orders, next, nil
}

// CancelOrder cancels the order and emits OrderCancelled.
func (s *Service) CancelOrder(ctx context.Context, id, reason string) (*domain.Order, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	o, err := s.repo.Update(ctx, id, nil, func(o *domain.Order) ([]outbox.Message, error) {
		if err := o.Cancel(reason, s.now()); err != nil {
			return nil, err
		}
		evt := &eventsv1.OrderCancelled{OrderId: o.ID, CustomerId: o.CustomerID, Reason: reason}
		evt.Metadata = events.NewMetadata(ctx, ServiceName, evt)
		msg, err := events.ToOutbox(events.TopicOrders, o.ID, evt)
		if err != nil {
			return nil, err
		}
		return []outbox.Message{msg}, nil
	})
	if err != nil {
		return nil, err
	}
	s.afterChange(ctx, o)
	return o, nil
}

// ApplyPaymentCompleted is called by the Kafka consumer. The eventID makes it
// idempotent: a redelivered event returns domain.ErrAlreadyProcessed.
func (s *Service) ApplyPaymentCompleted(ctx context.Context, eventID, orderID string) error {
	o, err := s.repo.Update(ctx, orderID, &InboxKey{Consumer: ServiceName, EventID: eventID},
		func(o *domain.Order) ([]outbox.Message, error) { return nil, o.MarkPaid(s.now()) })
	if err != nil {
		return err
	}
	s.afterChange(ctx, o)
	return nil
}

// ApplyPaymentFailed is the failure counterpart of ApplyPaymentCompleted.
func (s *Service) ApplyPaymentFailed(ctx context.Context, eventID, orderID, reason string) error {
	o, err := s.repo.Update(ctx, orderID, &InboxKey{Consumer: ServiceName, EventID: eventID},
		func(o *domain.Order) ([]outbox.Message, error) { return nil, o.MarkPaymentFailed(reason, s.now()) })
	if err != nil {
		return err
	}
	s.afterChange(ctx, o)
	return nil
}

// Watch subscribes to status changes of one order.
func (s *Service) Watch(orderID string) (<-chan *domain.Order, func()) {
	return s.broker.Subscribe(orderID)
}

// afterChange runs after a successful commit: refresh cache, notify watchers.
func (s *Service) afterChange(ctx context.Context, o *domain.Order) {
	s.cache.Put(o.ID, o.Clone())
	s.broker.Publish(o)
	s.log.InfoContext(ctx, "order status changed", "order_id", o.ID, "status", string(o.Status), "version", o.Version)
}

func validateID(id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return &domain.ValidationError{Field: "order_id", Message: "must be a UUID"}
	}
	return nil
}

// The page token is opaque to clients (base64 of JSON), so we can change the
// pagination strategy later without breaking them.
func encodeCursor(c Cursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(token string) (*Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(c.ID); err != nil || c.CreatedAt.IsZero() {
		return nil, errors.New("invalid cursor")
	}
	return &c, nil
}
