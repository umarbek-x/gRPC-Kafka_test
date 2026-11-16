//go:build e2e

// End-to-end tests: the whole system is running (3 services + Postgres +
// Kafka) and we only talk to the public gRPC API, like a real client.
//
//	docker compose --profile apps up -d --build
//	go test -tags=e2e ./test/e2e/...
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	orderv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/order/v1"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func client(t *testing.T) orderv1.OrderServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(envOr("ORDER_ADDR", "localhost:50061"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return orderv1.NewOrderServiceClient(conn)
}

func create(t *testing.T, c orderv1.OrderServiceClient, ctx context.Context, customer string, priceMinor int64) *orderv1.Order {
	t.Helper()
	resp, err := c.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		CustomerId: customer, Currency: "USD", IdempotencyKey: uuid.NewString(),
		Items: []*orderv1.OrderItem{{ProductId: "sku-e2e", Quantity: 1, UnitPriceMinor: priceMinor}},
	})
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	return resp.GetOrder()
}

// watchUntilSettled follows the stream until the server closes it.
func watchUntilSettled(t *testing.T, c orderv1.OrderServiceClient, ctx context.Context, id string) *orderv1.Order {
	t.Helper()
	stream, err := c.WatchOrder(ctx, &orderv1.WatchOrderRequest{OrderId: id})
	if err != nil {
		t.Fatalf("WatchOrder: %v", err)
	}
	var last *orderv1.Order
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return last
		}
		if err != nil {
			t.Fatalf("watch stream: %v (last=%v)", err, last)
		}
		last = msg.GetOrder()
	}
}

// Happy path through every component:
// gRPC -> Postgres+outbox -> Kafka -> payment-service -> Kafka -> order-service -> stream
func TestOrderIsPaid(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	o := create(t, c, ctx, "e2e-"+uuid.NewString(), 4_999)
	if o.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatalf("new order status = %s", o.GetStatus())
	}
	final := watchUntilSettled(t, c, ctx, o.GetId())
	if final.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PAID {
		t.Fatalf("final status = %s, want PAID", final.GetStatus())
	}
}

// The fake gateway declines amounts above 1000.00.
func TestExpensiveOrderFailsPayment(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	o := create(t, c, ctx, "e2e-"+uuid.NewString(), 250_000)
	final := watchUntilSettled(t, c, ctx, o.GetId())
	if final.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PAYMENT_FAILED || final.GetFailureReason() != "insufficient_funds" {
		t.Fatalf("final = %s (%s), want PAYMENT_FAILED (insufficient_funds)", final.GetStatus(), final.GetFailureReason())
	}
}

// The notification-service must have "sent" messages for our customer.
func TestNotificationsAreSent(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	customer := "e2e-" + uuid.NewString()
	o := create(t, c, ctx, customer, 1_000)
	watchUntilSettled(t, c, ctx, o.GetId())

	url := envOr("NOTIFICATION_OPS_URL", "http://localhost:18083") + "/debug/notifications"
	deadline := time.Now().Add(20 * time.Second)
	for {
		subjects := map[string]bool{}
		resp, err := http.Get(url)
		if err == nil {
			var list []struct {
				CustomerID string `json:"customer_id"`
				Subject    string `json:"subject"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&list)
			resp.Body.Close()
			for _, n := range list {
				if n.CustomerID == customer {
					subjects[n.Subject] = true
				}
			}
		}
		if subjects["We received your order"] && subjects["Payment confirmed"] {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected 2 notifications for %s, got %v (err=%v)", customer, subjects, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// Cancelling a paid order triggers a refund in the payment-service (saga
// compensation); the order ends up CANCELLED.
func TestCancelPaidOrder(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	o := create(t, c, ctx, "e2e-"+uuid.NewString(), 2_000)
	watchUntilSettled(t, c, ctx, o.GetId())

	resp, err := c.CancelOrder(ctx, &orderv1.CancelOrderRequest{OrderId: o.GetId(), Reason: "e2e test"})
	if err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if resp.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
		t.Fatalf("status = %s, want CANCELLED", resp.GetOrder().GetStatus())
	}
}
