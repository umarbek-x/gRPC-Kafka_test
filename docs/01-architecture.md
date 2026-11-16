# 1. Architecture

## The three services

| Service | Talks via | Owns | Job |
|---|---|---|---|
| **order-service** | gRPC (in), Kafka (in + out) | `orders` database | Order lifecycle. The only service with a synchronous API. |
| **payment-service** | Kafka (in + out) | `payments` database | Charges and refunds through a payment gateway. |
| **notification-service** | Kafka (in) | nothing (stateless) | Tells the customer what happened. |

Two communication styles are used on purpose:

- **Synchronous (gRPC)**: the caller waits for an answer. Used where a human or client needs the result *now* (`CreateOrder` returns the order).
- **Asynchronous (Kafka events)**: the producer announces a fact and does not know or care who listens. Used between services.

Why events between services? Consider the alternative: order-service calls payment-service over gRPC, which calls notification-service. If notification-service is down, creating an order fails, even though sending an email is not essential to taking an order. With events, order-service only needs its own database to be up. Kafka holds the events until the other services are ready. Services are **decoupled in time**.

The cost is **eventual consistency**: for a short moment the order is `PENDING` although the payment may already have succeeded. The API is honest about that (`CreateOrder` returns `PENDING`), and `WatchOrder` lets the client follow along.

## Rules the services follow

1. **Database per service.** No service reads another service's tables. The only shared contracts are the `.proto` files in `api/proto`.
2. **Events are facts in the past tense** (`OrderCreated`, not `CreateOrder`). The producer does not tell consumers what to do.
3. **Every consumer is idempotent.** Kafka can deliver a message twice; processing it twice must have the same effect as once.
4. **State change + event are atomic** (the outbox pattern, see [doc 4](04-postgres-and-outbox.md)).
5. **Same key, same partition, same order.** All events of one order use the order ID as the Kafka key.

## The saga: a transaction across services

There is no database transaction that spans two services. Instead, a business process is a chain of local transactions connected by events, called a **saga**:

```
order-service            payment-service              order-service
─────────────            ───────────────              ─────────────
TX: insert order   ──►   charge gateway         ──►   TX: order = PAID
    + OrderCreated       TX: insert payment               (or PAYMENT_FAILED)
                             + PaymentCompleted
```

You cannot roll back another service's committed work. If something must be undone you issue a **compensating action**: cancelling a paid order emits `OrderCancelled`, and payment-service reacts with a refund (`internal/payment/handler.go`, `onOrderCancelled`).

This style, where each service reacts to events without a central coordinator, is called **choreography**. The alternative, **orchestration**, has one service that tells the others what to do step by step; it is easier to follow for long, complex flows.

## Layers inside a service (clean / hexagonal architecture)

Open `internal/order/` — each folder is one layer:

```
        gRPC adapter                Kafka adapter
        grpcapi/server.go           eventhandler/handler.go
                 \                    /
                  ▼                  ▼
              service/service.go          ← use cases ("create order", "apply payment")
                       │
                       ▼
               domain/order.go            ← business rules, state machine. Pure Go.
                       ▲
                       │ implements service.Repository
          repository/postgres.go          ← Postgres adapter
```

**The dependency rule: arrows point inward.** `domain` imports nothing from the project. `service` imports `domain`. The adapters import `service`. Nothing in `domain` or `service` knows that gRPC, Kafka or SQL exist.

What that buys you:

- **Fast tests.** `domain` tests need no database. `service` tests use an in-memory fake repository (`service/memrepo.go`).
- **Replaceable infrastructure.** Switching to MongoDB means writing one new file that implements `service.Repository`. Nothing else changes.
- **One place for each kind of decision.** "Can a paid order be cancelled?" is answered only in `domain/order.go`.

The key Go idiom is in `service/service.go`: the **consumer of an interface defines it**. `service` declares `type Repository interface{...}` describing what it needs; `repository.Postgres` satisfies it. The line `var _ service.Repository = (*Postgres)(nil)` makes the compiler verify that.

### `internal/` vs `pkg/`

- `internal/` is enforced by the Go compiler: code there cannot be imported from outside this module. Business logic goes here.
- `pkg/` holds generic building blocks with no business knowledge (a Kafka consumer, an LRU cache). In a larger company these would become a shared platform library.

## The composition root

`cmd/order-service/main.go` is the only file that knows all the concrete types. It builds everything by hand, top to bottom:

```
config → logger → Postgres pool → migrations → Kafka producer
       → repository → service → gRPC server, outbox relay, Kafka consumer
       → start goroutines → wait for signal → graceful shutdown
```

No dependency-injection framework is needed; passing dependencies as constructor arguments *is* dependency injection.

## Life of one request, file by file

`shopctl create -customer alice -price 1999`

| Step | File | What happens |
|---|---|---|
| 1 | `cmd/shopctl/main.go` | Builds a `CreateOrderRequest`, sets a 5s deadline and an `x-request-id`, calls the server. |
| 2 | `pkg/grpcx/interceptors.go` | `UnaryRequestID` stores the request ID in the context as the correlation ID. `UnaryLogging` starts a timer. |
| 3 | `internal/order/grpcapi/server.go` | Converts protobuf → domain types, calls the service. |
| 4 | `internal/order/service/service.go` | `domain.NewOrder` validates and computes the total. Builds the `OrderCreated` event. |
| 5 | `internal/order/repository/postgres.go` | One transaction: insert order, insert items, insert outbox row. |
| 6 | `pkg/outbox/outbox.go` | The relay goroutine finds the row, publishes to `orders.v1`, marks it published. |
| 7 | `pkg/kafka/consumer.go` | payment-service fetches the message, routes it to a worker by key. |
| 8 | `internal/payment/handler.go` | Charges the gateway, saves the payment + `PaymentCompleted` in one transaction. |
| 9 | `internal/order/eventhandler/handler.go` | order-service consumes `PaymentCompleted`, calls `ApplyPaymentCompleted`. |
| 10 | `internal/order/watch/broker.go` | The change is fanned out to every `WatchOrder` stream of that order. |
| 11 | `internal/notification/` | Two notifications are scheduled by priority and sent under the rate limit. |

The **correlation ID** from step 1 travels the whole way: gRPC metadata → context → event metadata → Kafka header → the consumer's context → its log lines. Search the logs of all three services for one ID and you see the full story:

```bash
docker compose --profile apps logs | grep <correlation-id>
```

Next: [gRPC](02-grpc.md)
