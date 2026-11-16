//go:build integration

// Integration tests: run the REAL Postgres adapter against a REAL database.
// Unit tests with fakes cannot catch SQL mistakes, constraint behaviour or
// locking bugs; these can.
//
// The build tag keeps them out of `go test ./...`. Run them with:
//
//	docker compose up -d
//	go test -tags=integration ./test/integration/...
package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/domain"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/repository"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/service"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/postgres"
)

var pool *pgxpool.Pool

// TestMain runs once per package: connect, migrate, run tests, clean up.
func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://orderflow:orderflow@localhost:55432/orders_test?sslmode=disable"
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	if pool, err = postgres.Connect(ctx, dsn, 10, log); err != nil {
		fmt.Fprintln(os.Stderr, "integration tests need Postgres (docker compose up -d):", err)
		os.Exit(1)
	}
	if err = postgres.Migrate(ctx, pool, repository.Migrations, repository.MigrationsDir, log); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

// Each test uses a unique customer so tests don't interfere with each other
// or with data left by manual experiments (no "truncate everything" needed).
func newOrder(t *testing.T, customer, key string) *domain.Order {
	t.Helper()
	o, err := domain.NewOrder(uuid.NewString(), customer, "USD", key, []domain.Item{
		{ProductID: "sku-1", Quantity: 2, UnitPriceMinor: 1000},
		{ProductID: "sku-2", Quantity: 1, UnitPriceMinor: 550},
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func event(orderID string) outbox.Message {
	return outbox.Message{EventID: uuid.NewString(), Topic: "orders.v1", Key: orderID,
		EventType: "test.Event", Payload: []byte(`{}`), Headers: map[string]string{"event-type": "test.Event"}}
}

func countOutbox(t *testing.T, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE msg_key = $1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCreateAndGetRoundTrip(t *testing.T) {
	repo := repository.New(pool)
	ctx := context.Background()
	o := newOrder(t, "cust-"+uuid.NewString(), "")

	if _, created, err := repo.Create(ctx, o, []outbox.Message{event(o.ID)}); err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	got, err := repo.Get(ctx, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalMinor != 2550 || len(got.Items) != 2 || got.Items[0].ProductID != "sku-1" ||
		got.Status != domain.StatusPending || !got.CreatedAt.Equal(o.CreatedAt) {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if countOutbox(t, o.ID) != 1 {
		t.Fatal("outbox row must be written with the order")
	}
	if _, err := repo.Get(ctx, uuid.NewString()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// 20 goroutines retry the same request at once. The UNIQUE constraint must
// let exactly one insert win; everyone gets the same order back.
func TestConcurrentIdempotentCreate(t *testing.T) {
	repo := repository.New(pool)
	customer, key := "cust-"+uuid.NewString(), "retry-key"

	const n = 20
	ids := make([]string, n)
	createdCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o := newOrder(t, customer, key)
			saved, created, err := repo.Create(context.Background(), o, []outbox.Message{event(o.ID)})
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			mu.Lock()
			ids[i] = saved.ID
			if created {
				createdCount++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if createdCount != 1 {
		t.Fatalf("created %d orders, want exactly 1", createdCount)
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("callers saw different orders: %v", ids)
		}
	}
	if countOutbox(t, ids[0]) != 1 {
		t.Fatal("exactly one event must be emitted")
	}
}

func TestUpdateIsAtomicWithInboxAndOutbox(t *testing.T) {
	repo := repository.New(pool)
	ctx := context.Background()
	o := newOrder(t, "cust-"+uuid.NewString(), "")
	if _, _, err := repo.Create(ctx, o, nil); err != nil {
		t.Fatal(err)
	}
	inbox := &service.InboxKey{Consumer: "test", EventID: uuid.NewString()}
	markPaid := func(o *domain.Order) ([]outbox.Message, error) {
		return []outbox.Message{event(o.ID)}, o.MarkPaid(time.Now())
	}

	updated, err := repo.Update(ctx, o.ID, inbox, markPaid)
	if err != nil || updated.Status != domain.StatusPaid || updated.Version != 2 {
		t.Fatalf("update: %+v %v", updated, err)
	}
	// Same event again -> rejected by the inbox, nothing changes.
	if _, err := repo.Update(ctx, o.ID, inbox, markPaid); !errors.Is(err, domain.ErrAlreadyProcessed) {
		t.Fatalf("want ErrAlreadyProcessed, got %v", err)
	}
	if countOutbox(t, o.ID) != 1 {
		t.Fatal("duplicate must not emit a second event")
	}

	// A failing update rolls back EVERYTHING, including its inbox row, so the
	// event can be retried later.
	inbox2 := &service.InboxKey{Consumer: "test", EventID: uuid.NewString()}
	if _, err := repo.Update(ctx, o.ID, inbox2, markPaid); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("PAID -> PAID must fail, got %v", err)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM inbox WHERE event_id = $1`, inbox2.EventID).Scan(&n)
	if n != 0 {
		t.Fatal("inbox row must be rolled back with the failed update")
	}
}

// Two transactions race to move a PENDING order to different states. The
// row lock (SELECT ... FOR UPDATE) must serialize them: exactly one wins.
func TestConcurrentUpdatesAreSerialized(t *testing.T) {
	repo := repository.New(pool)
	ctx := context.Background()
	o := newOrder(t, "cust-"+uuid.NewString(), "")
	if _, _, err := repo.Create(ctx, o, nil); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 2)
	go func() {
		_, err := repo.Update(ctx, o.ID, nil, func(o *domain.Order) ([]outbox.Message, error) { return nil, o.MarkPaid(time.Now()) })
		errs <- err
	}()
	go func() {
		_, err := repo.Update(ctx, o.ID, nil, func(o *domain.Order) ([]outbox.Message, error) {
			return nil, o.MarkPaymentFailed("declined", time.Now())
		})
		errs <- err
	}()
	var ok, rejected int
	for i := 0; i < 2; i++ {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, domain.ErrInvalidTransition):
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if ok != 1 || rejected != 1 {
		t.Fatalf("ok=%d rejected=%d, want 1 and 1", ok, rejected)
	}
	final, _ := repo.Get(ctx, o.ID)
	if final.Version != 2 {
		t.Fatalf("version = %d, want 2 (exactly one transition applied)", final.Version)
	}
}

func TestListKeysetPagination(t *testing.T) {
	repo := repository.New(pool)
	ctx := context.Background()
	customer := "cust-" + uuid.NewString()
	base := time.Now().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		o, _ := domain.NewOrder(uuid.NewString(), customer, "USD", "", []domain.Item{{ProductID: "p", Quantity: 1, UnitPriceMinor: 1}},
			base.Add(time.Duration(i/2)*time.Second)) // pairs share a timestamp: exercises the id tie-break
		if _, _, err := repo.Create(ctx, o, nil); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}
	var after *service.Cursor
	for {
		page, err := repo.List(ctx, customer, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, o := range page {
			if seen[o.ID] {
				t.Fatalf("order %s listed twice", o.ID)
			}
			if len(o.Items) != 1 {
				t.Fatal("items must be loaded for listed orders")
			}
			seen[o.ID] = true
		}
		last := page[len(page)-1]
		after = &service.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if len(seen) != 5 {
		t.Fatalf("listed %d orders, want 5", len(seen))
	}
}

// capturePublisher stands in for Kafka.
type capturePublisher struct {
	mu   sync.Mutex
	msgs []kafka.Message
	fail bool
}

func (c *capturePublisher) Publish(_ context.Context, msgs ...kafka.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return errors.New("kafka unavailable")
	}
	c.msgs = append(c.msgs, msgs...)
	return nil
}

func (c *capturePublisher) has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.msgs {
		if m.Key == key {
			return true
		}
	}
	return false
}

func TestOutboxRelayPublishesAndMarksRows(t *testing.T) {
	repo := repository.New(pool)
	o := newOrder(t, "cust-"+uuid.NewString(), "")
	if _, _, err := repo.Create(context.Background(), o, []outbox.Message{event(o.ID)}); err != nil {
		t.Fatal(err)
	}

	pub := &capturePublisher{}
	relay := outbox.NewRelay(pool, pub, outbox.RelayConfig{PollInterval: 20 * time.Millisecond}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = relay.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(5 * time.Second)
	for !pub.has(o.ID) {
		if time.Now().After(deadline) {
			t.Fatal("relay did not publish the outbox message")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The row must be marked so it is not published again.
	deadline = time.Now().Add(5 * time.Second)
	for {
		var unpublished int
		_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE msg_key = $1 AND published_at IS NULL`, o.ID).Scan(&unpublished)
		if unpublished == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox row was not marked as published")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
