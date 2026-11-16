// shopctl is a small gRPC CLIENT for the order-service. Use it to explore the
// system and as a reference for writing gRPC clients in Go.
//
//	shopctl create -customer alice -product sku-1 -qty 2 -price 1999
//	shopctl get    -id <order-id>
//	shopctl list   -customer alice
//	shopctl cancel -id <order-id> -reason "changed my mind"
//	shopctl watch  -id <order-id>
//	shopctl load   -n 500 -c 20        (load generator using a worker pool)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orderv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/order/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/workerpool"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]

	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	addr := fs.String("addr", envOr("ORDER_ADDR", "localhost:50061"), "order-service address")
	timeout := fs.Duration("timeout", 5*time.Second, "per-request deadline")
	id := fs.String("id", "", "order ID")
	customer := fs.String("customer", "alice", "customer ID")
	product := fs.String("product", "sku-1", "product ID")
	qty := fs.Int("qty", 1, "quantity")
	price := fs.Int64("price", 1999, "unit price in minor units (cents)")
	currency := fs.String("currency", "USD", "ISO currency code")
	key := fs.String("key", "", "idempotency key (random if empty)")
	reason := fs.String("reason", "requested by customer", "cancel reason")
	n := fs.Int("n", 200, "load: number of orders")
	c := fs.Int("c", 16, "load: concurrent workers")
	_ = fs.Parse(args)

	// One ClientConn is a pool of HTTP/2 connections; it is safe for
	// concurrent use and should be created once and shared, never per request.
	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // TLS in production!
		// Client-side retry policy: retry UNAVAILABLE with backoff. Safe only
		// because our RPCs are idempotent (CreateOrder has an idempotency key).
		grpc.WithDefaultServiceConfig(`{"methodConfig":[{"name":[{"service":"orderflow.order.v1.OrderService"}],
			"retryPolicy":{"maxAttempts":4,"initialBackoff":"0.1s","maxBackoff":"1s","backoffMultiplier":2,
			"retryableStatusCodes":["UNAVAILABLE"]}}]}`))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	client := orderv1.NewOrderServiceClient(conn)

	// Ctrl+C cancels the context, which cancels the in-flight RPC.
	root, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// ALWAYS set a deadline on RPCs. Without one, a stuck server holds your
	// goroutine (and its memory) forever. The deadline travels to the server
	// and to anything the server calls with the same context.
	call := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithTimeout(root, *timeout)
		// Send our own request ID so we can find this call in server logs.
		return metadata.AppendToOutgoingContext(ctx, "x-request-id", uuid.NewString()), cancel
	}

	switch cmd {
	case "create":
		if *key == "" {
			*key = uuid.NewString()
		}
		ctx, cancel := call()
		defer cancel()
		resp, err := client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
			CustomerId: *customer, Currency: *currency, IdempotencyKey: *key,
			Items: []*orderv1.OrderItem{{ProductId: *product, Quantity: int32(*qty), UnitPriceMinor: *price}},
		})
		check(err)
		printMsg(resp)
	case "get":
		ctx, cancel := call()
		defer cancel()
		resp, err := client.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: *id})
		check(err)
		printMsg(resp)
	case "list":
		token := ""
		for page := 1; ; page++ {
			ctx, cancel := call()
			resp, err := client.ListOrders(ctx, &orderv1.ListOrdersRequest{CustomerId: *customer, PageSize: 10, PageToken: token})
			cancel()
			check(err)
			for _, o := range resp.GetOrders() {
				fmt.Printf("%s  %-15s %8d %s  %s\n", o.GetId(), shortStatus(o.GetStatus()), o.GetTotalMinor(), o.GetCurrency(),
					o.GetCreatedAt().AsTime().Format(time.RFC3339))
			}
			if token = resp.GetNextPageToken(); token == "" {
				break
			}
			fmt.Printf("--- page %d ---\n", page+1)
		}
	case "cancel":
		ctx, cancel := call()
		defer cancel()
		resp, err := client.CancelOrder(ctx, &orderv1.CancelOrderRequest{OrderId: *id, Reason: *reason})
		check(err)
		printMsg(resp)
	case "watch":
		// A stream lives longer than one request: use a longer deadline.
		ctx, cancel := context.WithTimeout(root, 2*time.Minute)
		defer cancel()
		stream, err := client.WatchOrder(ctx, &orderv1.WatchOrderRequest{OrderId: *id})
		check(err)
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				fmt.Println("stream closed by server (order settled)")
				return
			}
			check(err)
			o := msg.GetOrder()
			fmt.Printf("%s  status=%s version=%d %s\n", time.Now().Format("15:04:05.000"), shortStatus(o.GetStatus()), o.GetVersion(), o.GetFailureReason())
		}
	case "load":
		load(root, client, *n, *c, *timeout)
	default:
		usage()
		os.Exit(2)
	}
}

// load creates n orders using c concurrent workers, then waits for each order
// to be settled by the payment-service, and prints latency percentiles.
//
// Concurrency pattern: a bounded worker pool does the work, a mutex-protected
// slice collects results, atomic counters count outcomes.
func load(ctx context.Context, client orderv1.OrderServiceClient, n, workers int, timeout time.Duration) {
	var (
		mu                    sync.Mutex
		createLat, settleLat  []time.Duration
		paid, failed, errored atomic.Int64
	)
	pool := workerpool.New(workers, workers, nil)
	start := time.Now()

	for i := 0; i < n; i++ {
		i := i
		err := pool.Submit(ctx, func() {
			// Every 10th order exceeds the fake gateway's limit -> PAYMENT_FAILED.
			price := int64(1000 + i%50*100)
			if i%10 == 9 {
				price = 250_000
			}
			cctx, cancel := context.WithTimeout(ctx, timeout)
			t0 := time.Now()
			resp, err := client.CreateOrder(cctx, &orderv1.CreateOrderRequest{
				CustomerId: fmt.Sprintf("load-%d", i%25), Currency: "USD", IdempotencyKey: uuid.NewString(),
				Items: []*orderv1.OrderItem{{ProductId: "sku-load", Quantity: 1, UnitPriceMinor: price}},
			})
			cancel()
			if err != nil {
				errored.Add(1)
				return
			}
			created := time.Since(t0)

			// Follow the order to its final status via the streaming RPC.
			wctx, wcancel := context.WithTimeout(ctx, 60*time.Second)
			defer wcancel()
			stream, err := client.WatchOrder(wctx, &orderv1.WatchOrderRequest{OrderId: resp.GetOrder().GetId()})
			if err != nil {
				errored.Add(1)
				return
			}
			var final orderv1.OrderStatus
			for {
				msg, err := stream.Recv()
				if err != nil {
					break
				}
				final = msg.GetOrder().GetStatus()
			}
			switch final {
			case orderv1.OrderStatus_ORDER_STATUS_PAID:
				paid.Add(1)
			case orderv1.OrderStatus_ORDER_STATUS_PAYMENT_FAILED:
				failed.Add(1)
			default:
				errored.Add(1)
				return
			}
			mu.Lock()
			createLat = append(createLat, created)
			settleLat = append(settleLat, time.Since(t0))
			mu.Unlock()
		})
		if err != nil {
			break // ctx cancelled (Ctrl+C)
		}
	}
	pool.Close() // wait for all workers to finish
	elapsed := time.Since(start)

	fmt.Printf("\norders: %d   workers: %d   elapsed: %s   throughput: %.0f orders/s\n", n, workers, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
	fmt.Printf("paid: %d   payment_failed: %d   errors: %d\n", paid.Load(), failed.Load(), errored.Load())
	report("CreateOrder RPC latency ", createLat)
	report("create -> settled (e2e) ", settleLat)
}

func report(name string, lat []time.Duration) {
	if len(lat) == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)].Round(100 * time.Microsecond) }
	fmt.Printf("%s p50=%-9s p95=%-9s p99=%-9s max=%s\n", name, pct(0.50), pct(0.95), pct(0.99), lat[len(lat)-1].Round(100*time.Microsecond))
}

func shortStatus(s orderv1.OrderStatus) string {
	const prefix = "ORDER_STATUS_"
	return s.String()[len(prefix):]
}

func printMsg(m proto.Message) {
	fmt.Println(protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: false}.Format(m))
}

// check prints a gRPC error the way a client should read it: code + message.
func check(err error) {
	if err == nil {
		return
	}
	st := status.Convert(err)
	fmt.Fprintf(os.Stderr, "error: code=%s message=%q\n", st.Code(), st.Message())
	for _, d := range st.Details() {
		fmt.Fprintf(os.Stderr, "  detail: %v\n", d)
	}
	if st.Code() == codes.Unavailable {
		fmt.Fprintln(os.Stderr, "  hint: is the order-service running?")
	}
	os.Exit(1)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fatal:", err)
	os.Exit(1)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: shopctl <command> [flags]

commands:
  create  -customer -product -qty -price -currency [-key]
  get     -id
  list    -customer
  cancel  -id [-reason]
  watch   -id
  load    -n <orders> -c <workers>

common flags: -addr localhost:50061  -timeout 5s`)
}
