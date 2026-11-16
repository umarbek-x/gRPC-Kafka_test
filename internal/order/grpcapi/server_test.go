package grpcapi

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	orderv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/order/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/service"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/watch"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/grpcx"
)

// startServer runs a REAL gRPC server over an in-memory connection (bufconn):
// requests go through protobuf serialization, interceptors and status
// mapping, exactly like production, but without opening a TCP port.
func startServer(t *testing.T) (orderv1.OrderServiceClient, *service.Service) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := service.New(service.NewMemoryRepository(), watch.NewBroker(), 100, time.Minute, log)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpcx.UnaryRequestID(), grpcx.UnaryLogging(log), grpcx.UnaryRecovery(log)),
		grpc.ChainStreamInterceptor(grpcx.StreamRequestID(), grpcx.StreamLogging(log), grpcx.StreamRecovery(log)),
	)
	orderv1.RegisterOrderServiceServer(srv, NewServer(svc, log))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})
	return orderv1.NewOrderServiceClient(conn), svc
}

func createReq(key string) *orderv1.CreateOrderRequest {
	return &orderv1.CreateOrderRequest{
		CustomerId: "cust-1", Currency: "USD", IdempotencyKey: key,
		Items: []*orderv1.OrderItem{{ProductId: "sku-1", Quantity: 3, UnitPriceMinor: 700}},
	}
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCreateAndGet(t *testing.T) {
	client, _ := startServer(t)
	ctx := ctxT(t)

	var header metadata.MD
	created, err := client.CreateOrder(ctx, createReq("k1"), grpc.Header(&header))
	if err != nil {
		t.Fatal(err)
	}
	if !created.GetCreated() || created.GetOrder().GetTotalMinor() != 2100 ||
		created.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatalf("unexpected response: %v", created)
	}
	if len(header.Get(grpcx.RequestIDHeader)) == 0 {
		t.Fatal("server must return x-request-id header")
	}

	got, err := client.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: created.GetOrder().GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetOrder().GetId() != created.GetOrder().GetId() || len(got.GetOrder().GetItems()) != 1 {
		t.Fatalf("get mismatch: %v", got)
	}

	replay, err := client.CreateOrder(ctx, createReq("k1"))
	if err != nil || replay.GetCreated() || replay.GetOrder().GetId() != created.GetOrder().GetId() {
		t.Fatalf("idempotent replay failed: %v %v", replay, err)
	}
}

func TestErrorCodes(t *testing.T) {
	client, _ := startServer(t)
	ctx := ctxT(t)

	t.Run("invalid argument carries field details", func(t *testing.T) {
		req := createReq("k")
		req.Currency = "dollars"
		_, err := client.CreateOrder(ctx, req)
		st := status.Convert(err)
		if st.Code() != codes.InvalidArgument {
			t.Fatalf("code = %s", st.Code())
		}
		var field string
		for _, d := range st.Details() {
			if br, ok := d.(*errdetails.BadRequest); ok && len(br.GetFieldViolations()) > 0 {
				field = br.GetFieldViolations()[0].GetField()
			}
		}
		if field != "currency" {
			t.Fatalf("expected BadRequest detail for currency, got %q", field)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, err := client.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: "6f1e6f0e-58a1-4c0b-9d57-0a4f5e3a9b11"})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("code = %s", status.Code(err))
		}
	})

	t.Run("malformed id", func(t *testing.T) {
		_, err := client.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: "42"})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %s", status.Code(err))
		}
	})

	t.Run("cancel twice is failed precondition", func(t *testing.T) {
		o, _ := client.CreateOrder(ctx, createReq("k-cancel"))
		id := o.GetOrder().GetId()
		if _, err := client.CancelOrder(ctx, &orderv1.CancelOrderRequest{OrderId: id, Reason: "x"}); err != nil {
			t.Fatal(err)
		}
		_, err := client.CancelOrder(ctx, &orderv1.CancelOrderRequest{OrderId: id, Reason: "x"})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("code = %s", status.Code(err))
		}
	})
}

func TestListOrdersPagination(t *testing.T) {
	client, _ := startServer(t)
	ctx := ctxT(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if _, err := client.CreateOrder(ctx, createReq(k)); err != nil {
			t.Fatal(err)
		}
	}
	total, token := 0, ""
	for {
		resp, err := client.ListOrders(ctx, &orderv1.ListOrdersRequest{CustomerId: "cust-1", PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		total += len(resp.GetOrders())
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	if total != 5 {
		t.Fatalf("listed %d orders, want 5", total)
	}
}

// Server-streaming RPC: first message is the current state, then the status
// change, then the server closes the stream (io.EOF on the client).
func TestWatchOrderStreamsStatusChange(t *testing.T) {
	client, svc := startServer(t)
	ctx := ctxT(t)

	created, err := client.CreateOrder(ctx, createReq("k-watch"))
	if err != nil {
		t.Fatal(err)
	}
	id := created.GetOrder().GetId()

	stream, err := client.WatchOrder(ctx, &orderv1.WatchOrderRequest{OrderId: id})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil || first.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING {
		t.Fatalf("first message: %v %v", first, err)
	}

	// Simulate the payment event arriving from Kafka.
	if err := svc.ApplyPaymentCompleted(ctx, "evt-1", id); err != nil {
		t.Fatal(err)
	}
	second, err := stream.Recv()
	if err != nil || second.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PAID {
		t.Fatalf("second message: %v %v", second, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream should end after a settled status, got %v", err)
	}
}

func TestWatchOrderStopsWhenClientCancels(t *testing.T) {
	client, _ := startServer(t)
	created, err := client.CreateOrder(ctxT(t), createReq("k-cancel-watch"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.WatchOrder(ctx, &orderv1.WatchOrderRequest{OrderId: created.GetOrder().GetId()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("want Canceled, got %v", err)
	}
}
