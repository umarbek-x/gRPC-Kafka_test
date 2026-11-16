# 7. Running and experiments

## Ports

| Port | What |
|---|---|
| 50061 | order-service gRPC |
| 18081 / 18082 / 18083 | ops HTTP (`/healthz`, `/readyz`, `/metrics`) of order / payment / notification |
| 18080 | Kafka UI |
| 19092 | Kafka (from your machine) |
| 55432 | Postgres (user `orderflow`, password `orderflow`; databases `orders`, `payments`, `orders_test`) |

## Two ways to run

### A. Everything in Docker (simplest)

```bash
docker compose --profile apps up -d --build      # start
docker compose --profile apps logs -f order-service payment-service notification-service
docker compose --profile apps down               # stop, keep data
docker compose --profile apps down -v            # stop and delete all data
```

### B. Infrastructure in Docker, services on your machine (best for development)

You can edit code, restart one service in a second, and attach a debugger.

```bash
docker compose up -d          # Postgres, Kafka, Kafka UI only

# three terminals:
go run ./cmd/order-service
go run ./cmd/payment-service
go run ./cmd/notification-service
```

The defaults in each `main.go` already point at the Docker infrastructure. Do not run A and B at the same time: the ports would clash.

To change a setting, set an environment variable before `go run` (the full list is in `.env.example`):

```powershell
# PowerShell
$env:LOG_LEVEL = "debug"; go run ./cmd/payment-service
```
```bash
# bash
LOG_LEVEL=debug go run ./cmd/payment-service
```

Stop a service with Ctrl+C and read the shutdown log lines.

## The client: `shopctl`

```bash
go run ./cmd/shopctl create -customer alice -product sku-1 -qty 2 -price 1999
go run ./cmd/shopctl get    -id <order-id>
go run ./cmd/shopctl list   -customer alice
go run ./cmd/shopctl watch  -id <order-id>
go run ./cmd/shopctl cancel -id <order-id> -reason "changed my mind"
go run ./cmd/shopctl load   -n 500 -c 20
```

`-price` is in cents. Orders above `100000` (1000.00) are declined by the fake gateway.

Build real binaries with `go build -o bin/ ./cmd/...`.

## All developer commands

The `Makefile` lists them (`make help`). Without `make`, run the command directly:

| Task | Command |
|---|---|
| Unit tests | `go test ./...` |
| Integration tests | `go test -tags=integration -count=1 ./test/integration/...` |
| End-to-end tests | `go test -tags=e2e -count=1 ./test/e2e/...` |
| Vet | `go vet ./...` |
| Format | `gofmt -w .` |
| Regenerate protobuf code | `bash scripts/gen-proto.sh` |
| Build binaries | `go build -o bin/ ./cmd/...` |

## Looking inside

**Kafka UI** — http://localhost:18080
Topics → messages with keys and headers. Consumers → members, assigned partitions, lag.

**Postgres**
```bash
docker compose exec postgres psql -U orderflow -d orders
```
```sql
SELECT id, status, total_minor, version FROM orders ORDER BY created_at DESC LIMIT 5;
SELECT id, event_type, published_at FROM outbox ORDER BY id DESC LIMIT 5;
SELECT * FROM inbox ORDER BY processed_at DESC LIMIT 5;
\c payments
SELECT order_id, status, amount_minor, failure_reason FROM payments ORDER BY created_at DESC LIMIT 5;
```

**Metrics**
```bash
curl -s localhost:18081/metrics | grep -E "^(grpc_server|outbox_|kafka_consumer)"
```

| Metric | Meaning |
|---|---|
| `grpc_server_requests_total{method,code}` | Request count by result. The error rate comes from here. |
| `grpc_server_request_seconds` | Latency histogram (p50/p95/p99). |
| `kafka_consumer_messages_total{group,topic,result}` | `ok`, `retried_ok`, `dlq`, `skipped`. Alert on `dlq`. |
| `kafka_consumer_handle_seconds` | Handler duration including retries. |
| `kafka_consumer_in_flight` | Fetched but not yet committed. |
| `outbox_published_total`, `outbox_publish_errors_total` | Relay health. |

**Logs** are JSON, one object per line. Follow one request through all services by its correlation ID:

```bash
docker compose --profile apps logs | grep <correlation-id>
```

The ID is the `x-request-id` that `shopctl` sends; you can find it as `correlation_id` in the order-service log line for your call.

## Guided experiments

Each one demonstrates a property the system was designed to have. Predict the result first, then run it.

### 1. A consumer is down: nothing is lost
```bash
docker compose stop payment-service
go run ./cmd/shopctl create -customer bob      # succeeds; the order stays PENDING
go run ./cmd/shopctl watch -id <id>            # leave this running
docker compose start payment-service           # in another terminal
```
The watch prints `PAID` moments after the service returns. Kafka kept the event.

### 2. Kafka is down: orders are still accepted
```bash
docker compose stop kafka
go run ./cmd/shopctl create -customer bob      # still succeeds
docker compose exec postgres psql -U orderflow -d orders -c "SELECT id, published_at FROM outbox ORDER BY id DESC LIMIT 3"
docker compose start kafka
```
`published_at` is empty while Kafka is down; afterwards the relay publishes the backlog and the order gets paid. That is the outbox pattern at work.

### 3. Retries
payment-service fails 10% of gateway calls on purpose.
```bash
go run ./cmd/shopctl load -n 200 -c 20
docker compose logs payment-service | grep "will retry" | head
curl -s localhost:18082/metrics | grep kafka_consumer_messages_total
```
All orders settle; `retried_ok` counts the ones that needed a second attempt. Now set `GATEWAY_FAILURE_RATE=0.9` (in `docker-compose.yml` under payment-service → `environment`, then `docker compose --profile apps up -d`) and find messages in `payment-service.dlq`.

### 4. Idempotent API
```bash
go run ./cmd/shopctl create -customer bob -key my-key-1
go run ./cmd/shopctl create -customer bob -key my-key-1
```
Same order ID both times; the second response has no `"created": true`. Only one event is in `orders.v1`.

### 5. Cancellation and refund (saga compensation)
```bash
go run ./cmd/shopctl create -customer bob -price 5000     # wait until PAID
go run ./cmd/shopctl cancel -id <id>
docker compose exec postgres psql -U orderflow -d payments -c "SELECT order_id, status FROM payments ORDER BY updated_at DESC LIMIT 1"
```
The payment is `REFUNDED`, and `/debug/notifications` shows "Refund issued".

### 6. Backpressure and priorities
Set `SEND_RATE_PER_SECOND: "5"` for notification-service and restart it. Run `shopctl load -n 300`. Orders still settle quickly (payment does not depend on notifications), while notification lag grows in Kafka UI and drains at 5 per second. In `/debug/notifications`, "payment failed" (priority 9) overtakes lower priorities.

### 7. Graceful shutdown under load
Start `shopctl load -n 1000 -c 30` and, while it runs, `docker compose restart payment-service`. Result: `errors: 0`. In-flight messages finish, offsets are committed, and the restarted service continues from there.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `code=Unavailable` from shopctl | order-service is not running, or `-addr` is wrong. |
| A service logs `postgres not ready, retrying` | Normal for a few seconds at startup. If it persists: `docker compose ps`. |
| Orders stay `PENDING` | payment-service is down, or look for errors: `docker compose logs payment-service`. |
| `bind: address already in use` | You are running both mode A and mode B. Stop one. |
| `Unknown Topic Or Partition` | Topics were not created: `docker compose up kafka-init`. |
| Schema is in a strange state after experiments | `docker compose --profile apps down -v` and start again. |

Next: [Production checklist](08-production-checklist.md)
