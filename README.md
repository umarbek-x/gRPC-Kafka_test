# gRPC-Kafka_test — OrderFlow

A small but production-style **event-driven microservices** system written in Go, built to be read and learned from.

It covers **gRPC**, **Kafka**, **PostgreSQL**, **goroutines**, **worker pools**, **data structures** and **testing** the way a real company would build them, and every non-obvious line of code has a comment explaining *why*.

```
                 gRPC                      Kafka: orders.v1                  Kafka: payments.v1
  shopctl ───────────────► order-service ─────────────────► payment-service ──────────────────┐
  (client)   CreateOrder    │  Postgres      OrderCreated     │  Postgres     PaymentCompleted │
             WatchOrder ◄───┤  (orders)      OrderCancelled   │  (payments)   PaymentFailed    │
              (stream)      │                                 │               PaymentRefunded  │
                            └──────────────◄──────────────────┼────────────────────────────────┤
                              consumes payments.v1            │                                │
                              (marks order PAID / FAILED)     ▼                                ▼
                                                        notification-service  ◄── orders.v1 + payments.v1
                                                        (priority queue + rate limiter, "sends" emails)
```

## What happens when you create an order

1. `shopctl` calls **`CreateOrder`** over gRPC.
2. **order-service** saves the order *and* an `OrderCreated` event in **one database transaction** (the *transactional outbox*).
3. A background goroutine (the *outbox relay*) publishes the event to Kafka topic `orders.v1`.
4. **payment-service** consumes it with a **worker pool**, charges the (fake) payment gateway, and publishes `PaymentCompleted` or `PaymentFailed` to `payments.v1`.
5. **order-service** consumes that and moves the order to `PAID` or `PAYMENT_FAILED`.
6. Clients connected through the **`WatchOrder` streaming RPC** see the change immediately.
7. **notification-service** consumes both topics and sends notifications, most important first, without exceeding the provider's rate limit.

## Quick start

Requirements: Go 1.26+ and Docker.

```bash
# 1. Start everything: Postgres, Kafka, Kafka UI and the three services
docker compose --profile apps up -d --build

# 2. Create an order (prints the order with its id)
go run ./cmd/shopctl create -customer alice -price 1999

# 3. Watch it get paid (use the id printed above)
go run ./cmd/shopctl watch -id <order-id>

# 4. An order above 1000.00 is declined by the fake gateway
go run ./cmd/shopctl create -customer alice -price 250000

# 5. Fire 500 orders with 20 concurrent workers and see latency percentiles
go run ./cmd/shopctl load -n 500 -c 20
```

Then look around:

| What | Where |
|---|---|
| Kafka UI (topics, messages, consumer groups, lag) | http://localhost:18080 |
| Last notifications sent | http://localhost:18083/debug/notifications |
| Prometheus metrics | http://localhost:18081/metrics (order), `:18082` (payment), `:18083` (notification) |
| Health / readiness | http://localhost:18081/healthz and `/readyz` |
| Service logs | `docker compose --profile apps logs -f order-service payment-service notification-service` |

> Host ports are intentionally unusual (55432, 19092, 18080, 50061, 1808x) so this stack does not collide with other things running on your machine.

## Tests

```bash
go test ./...                                          # unit tests, no dependencies
go test -tags=integration ./test/integration/...       # real Postgres  (needs: docker compose up -d)
go test -tags=e2e ./test/e2e/...                       # whole system   (needs: --profile apps up)
```

## Learning path

Read the docs in order; each one points at the files to open.

| # | Doc | You will learn |
|---|---|---|
| 1 | [Architecture](docs/01-architecture.md) | The services, the project layout, clean architecture, the life of a request |
| 2 | [gRPC](docs/02-grpc.md) | Protobuf, unary and streaming RPCs, interceptors, status codes, deadlines, health checks |
| 3 | [Kafka](docs/03-kafka.md) | Topics, partitions, keys, consumer groups, offsets, at-least-once, retries, dead-letter queues |
| 4 | [Postgres and the outbox](docs/04-postgres-and-outbox.md) | Transactions, locking, idempotency, keyset pagination, the outbox and inbox patterns |
| 5 | [Concurrency and data structures](docs/05-concurrency.md) | Goroutines, channels, context, worker pools, LRU, heap, ring buffer, token bucket |
| 6 | [Testing](docs/06-testing.md) | Unit, integration and end-to-end tests; fakes; table-driven tests; testing concurrency |
| 7 | [Running and experiments](docs/07-running.md) | Every command, plus guided experiments (kill a service, break Kafka, read the DLQ) |
| 8 | [Production checklist](docs/08-production-checklist.md) | What this project already does, and what a real deployment would add |

## Project layout

```
api/proto/            Protobuf contracts: the gRPC API and the Kafka events
gen/go/               Generated Go code (committed; regenerate with scripts/gen-proto.sh)
cmd/                  One main package per binary
  order-service/        gRPC API + outbox relay + Kafka consumer
  payment-service/      Kafka consumer + outbox relay
  notification-service/ Kafka consumer + scheduler
  shopctl/              CLI client and load generator
internal/             Private code of each service (business logic lives here)
  order/{domain,service,repository,grpcapi,eventhandler,watch}
  payment/
  notification/
pkg/                  Reusable building blocks shared by all services
  kafka/                producer, consumer (worker pool, offsets, retries, DLQ)
  outbox/               transactional outbox + relay
  workerpool/           bounded pool and keyed (ordered) pool
  ds/{lru,pqueue,ringbuffer}   data structures
  retry/ ratelimit/ grpcx/ postgres/ events/ observability/ lifecycle/ config/
test/                 Integration and end-to-end tests
deploy/               Postgres and Kafka init scripts
docs/                 The learning guide
```
