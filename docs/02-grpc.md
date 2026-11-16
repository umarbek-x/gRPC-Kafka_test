# 2. gRPC

gRPC is a way for programs to call functions on other machines. You describe the functions and their data in a `.proto` file, generate code for client and server, and call them like normal methods. Underneath, messages are binary **Protocol Buffers** sent over **HTTP/2**.

Compared with JSON over REST: the contract is explicit and enforced by generated code, messages are smaller and faster to parse, and streaming is built in. REST is still the better fit for public, browser-facing APIs.

## The contract: `api/proto/order/v1/order.proto`

```proto
service OrderService {
  rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse);          // unary
  rpc WatchOrder(WatchOrderRequest)   returns (stream WatchOrderResponse);    // server streaming
}
```

Things to notice in the file:

- **Field numbers are the real identity of a field** (`string customer_id = 1;`). The name is not sent on the wire, the number is. That gives the compatibility rules:
  - Adding a field with a new number is safe. Old code ignores it.
  - Never change a field's number or type, and never reuse a deleted number.
  - Breaking changes go in a new package: `orderflow.order.v2`.
- **Every RPC has its own request and response message**, even when they look alike (`GetOrderResponse` wraps `Order`). Later you can add a field to one response without affecting the others.
- **Enums start with `_UNSPECIFIED = 0`.** In proto3 a missing field equals the zero value, so zero must mean "not set", never a real state.
- **Money is `int64` minor units** (cents). Floats cannot represent 0.10 exactly.
- **`idempotency_key`** makes `CreateOrder` safe to retry. Without it, a client that times out cannot know whether to retry (risk: two orders) or give up (risk: no order).

Generate the Go code with `bash scripts/gen-proto.sh`. It writes into `gen/go/`:

- `order.pb.go`: the message structs (`orderv1.Order`, getters like `GetCustomerId()`).
- `order_grpc.pb.go`: the client (`OrderServiceClient`) and the server interface you implement.

Generated code is committed so the project builds without `protoc` installed.

## The server: `internal/order/grpcapi/server.go`

The handler is a thin translator: protobuf in → domain types → call the service → protobuf out. No business logic.

```go
type Server struct {
    orderv1.UnimplementedOrderServiceServer   // forward compatibility
    svc *service.Service
}
```

Embedding `Unimplemented...Server` means that when a new RPC is added to the proto, the server still compiles and returns `Unimplemented` for it until you write it.

### Status codes are part of the API

`toStatus` maps domain errors to gRPC codes. Clients decide what to do from the code:

| Situation | Code | What the client should do |
|---|---|---|
| Bad input | `InvalidArgument` | Fix the request. Do not retry. |
| No such order | `NotFound` | Do not retry. |
| Cancelling an already cancelled order | `FailedPrecondition` | The system is in the wrong state for this call. |
| Client gave up | `Canceled` / `DeadlineExceeded` | — |
| Server not reachable | `Unavailable` | Retry with backoff. |
| A bug or an unexpected failure | `Internal` | Report it. |

Two details worth copying:

- **Rich error details.** `InvalidArgument` carries a `BadRequest` detail listing the bad field, so a UI can highlight it. Try: `go run ./cmd/shopctl create -currency dollars`.
- **Never leak internals.** Unknown errors are logged on the server with full detail and returned to the client only as `"internal error"`.

### Server streaming: `WatchOrder`

The handler receives a `stream` and calls `stream.Send(...)` as many times as it likes; returning from the function closes the stream.

```go
updates, unsubscribe := s.svc.Watch(req.GetOrderId())   // 1. subscribe first
defer unsubscribe()                                      //    always clean up
current, err := s.svc.GetOrder(ctx, ...)                 // 2. then read current state
stream.Send(current)
for {
    select {
    case <-ctx.Done():  return ...                       // client left or deadline passed
    case o := <-updates: stream.Send(o) ...
    }
}
```

Two lessons live in those lines:

- **Subscribe before reading.** If you read first and subscribe second, a change in between is lost forever. Subscribing first can produce a duplicate instead, which the `Version` check filters out. Prefer "possible duplicate" over "possible loss".
- **Always select on `ctx.Done()`.** gRPC runs each stream handler in its own goroutine. If the handler does not return when the client disconnects, that goroutine and its subscription leak.

The four RPC kinds, for reference: unary (one request, one response), server streaming (used here), client streaming (the client sends many, e.g. an upload), and bidirectional streaming (both, e.g. chat).

## Interceptors: `pkg/grpcx/interceptors.go`

Interceptors are middleware. Each one wraps the next:

```
request → RequestID → Logging → Recovery → handler
```

- **RequestID** reads `x-request-id` from the metadata (gRPC's version of HTTP headers) or creates one, and stores it in the context.
- **Logging** writes one structured line per call and records Prometheus metrics (count by code, latency histogram). Server faults log at `ERROR`, client faults at `WARN`.
- **Recovery** turns a panic into `codes.Internal`. Without it one bad request crashes the process and every other in-flight request with it.

They are registered once in `cmd/order-service/main.go` with `grpc.ChainUnaryInterceptor(...)`. Streaming RPCs need their own versions (`ChainStreamInterceptor`); both sets are there.

In a real deployment this is also where **authentication** goes: an interceptor validates a token from the metadata and puts the user into the context.

## The client: `cmd/shopctl/main.go`

```go
conn, _ := grpc.NewClient(addr, ...)            // create ONCE, reuse, safe for concurrent use
client := orderv1.NewOrderServiceClient(conn)

ctx, cancel := context.WithTimeout(ctx, 5*time.Second)   // ALWAYS set a deadline
defer cancel()
resp, err := client.CreateOrder(ctx, req)
```

- **One `ClientConn` for the whole program.** It multiplexes many concurrent calls over HTTP/2. Creating one per request is a common and expensive mistake.
- **Deadlines.** Without one, a hung server keeps your goroutine forever. The deadline is sent to the server, and because the server passes the same `ctx` to its database calls, the whole chain is cancelled together.
- **Retries.** The client's service config retries `UNAVAILABLE` automatically with backoff. That is only safe because the RPCs are idempotent.
- **Reading errors.** `status.Convert(err)` gives you the code, message and details (`check()` in the same file).

## Operational features (in `cmd/order-service/main.go`)

- **Health service** (`grpc.health.v1.Health`): the standard protocol that Kubernetes and load balancers probe.
- **Reflection**: lets tools discover the API without the proto files. With [grpcurl](https://github.com/fullstorydev/grpcurl) installed:
  ```bash
  grpcurl -plaintext localhost:50061 list
  grpcurl -plaintext -d '{"customer_id":"alice"}' localhost:50061 orderflow.order.v1.OrderService/ListOrders
  ```
- **Keepalive and `MaxConnectionAge`**: gRPC connections live for a long time, so a load balancer that balances *connections* would keep sending everything to the same old replicas. Recycling connections periodically lets new replicas receive traffic.
- **Graceful stop**: `GracefulStop()` stops accepting new calls and waits for running ones, with a timeout after which `Stop()` forces it.

## Try it

1. Send a bad request and read the error: `go run ./cmd/shopctl create -qty 0`
2. Send the same idempotency key twice and compare the responses (`"created": true` is missing the second time):
   `go run ./cmd/shopctl create -key demo-1` (run it twice)
3. Stream a cancellation: `docker compose stop payment-service`, create an order (it stays `PENDING`), run `watch` on it in one terminal and `cancel` it from another. The stream prints `CANCELLED` and the server closes it. Start payment-service again afterwards.
4. Add a field `string note = 5;` to `CreateOrderRequest`, regenerate, and confirm the old client still works.

Next: [Kafka](03-kafka.md)
