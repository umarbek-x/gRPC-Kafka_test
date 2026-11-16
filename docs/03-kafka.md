# 3. Kafka

Kafka is a distributed **log**. Producers append messages to the end; consumers read forward at their own pace. Reading does not delete anything: messages stay for a configured time (7 days here), so several independent services can each read the full stream, and a service that was down can catch up.

That last point is the main difference from a classic message queue, where a message is handed to one consumer and then removed.

## The five ideas you need

### 1. Topic and partition

A **topic** is a named stream (`orders.v1`). It is split into **partitions**; each partition is an ordered, append-only sequence. Every message in a partition has an **offset** (0, 1, 2, ...).

```
orders.v1
  partition 0:  [0][1][2][3][4]            ← ordered
  partition 1:  [0][1][2]                  ← ordered
  partition 2:  [0][1][2][3][4][5][6]      ← ordered
  (no ordering BETWEEN partitions)
```

### 2. Key

The producer picks a partition by hashing the message **key**: `partition = hash(key) % partitions`. Same key, same partition, therefore **messages with the same key are read in the order they were written**.

In this project the key is always the **order ID** (`service.go`: `events.ToOutbox(events.TopicOrders, o.ID, evt)`). So `OrderCreated` is always consumed before `OrderCancelled` for the same order, while different orders spread across all partitions and are processed in parallel.

Choosing the key is the most important design decision for a topic. Ask: "which messages must stay in order relative to each other?"

### 3. Consumer group

Consumers with the same **group ID** share the work: Kafka gives each partition to exactly one member of the group. Start a second copy of payment-service and Kafka **rebalances**: each copy gets 3 of the 6 partitions.

Different groups are independent. Both `payment-service` and `notification-service` read `orders.v1`, and each gets every message.

```
orders.v1 (6 partitions)
   ├── group "payment-service"       instance A: p0 p1 p2    instance B: p3 p4 p5
   └── group "notification-service"  instance A: p0..p5
```

So the number of partitions is the upper limit on how many instances of one service can work in parallel.

### 4. Offset commit

A consumer periodically tells Kafka "group X has processed partition 2 up to offset 41". After a restart or rebalance, reading resumes from the committed offset.

**When** you commit decides your delivery guarantee:

| Commit... | If you crash in between | Guarantee |
|---|---|---|
| before processing | the message is skipped | at-most-once (can lose) |
| **after processing** | the message is processed again | **at-least-once (can duplicate)** |

This project commits after processing. Duplicates are therefore possible and every consumer must be **idempotent**.

"Exactly-once" in practice means *at-least-once delivery + idempotent processing*. See how each service achieves it in [doc 4](04-postgres-and-outbox.md).

### 5. Lag

Lag = newest offset in the partition − committed offset of the group. It is the number of messages waiting. Growing lag means consumers cannot keep up; it is the first Kafka metric to alert on. See it in Kafka UI → *Consumers*.

## Topics in this project

Created by `deploy/kafka/create-topics.sh` (auto-creation is disabled so a typo is an error, not a new topic).

| Topic | Key | Events | Producers | Consumer groups |
|---|---|---|---|---|
| `orders.v1` | order ID | `OrderCreated`, `OrderCancelled` | order-service | payment-service, notification-service |
| `payments.v1` | order ID | `PaymentCompleted`, `PaymentFailed`, `PaymentRefunded` | payment-service | order-service, notification-service |
| `<service>.dlq` | original key | messages that could not be processed | that service's consumer | (humans) |

## Events: `api/proto/events/v1/events.proto` and `pkg/events/events.go`

Events are defined in protobuf (one schema, same compatibility rules as the API) and serialized as JSON so you can read them in Kafka UI.

Every message carries **headers**:

| Header | Purpose |
|---|---|
| `event-type` | e.g. `orderflow.events.v1.OrderCreated`. Consumers route on it without parsing the body. |
| `event-id` | Unique per event. The de-duplication key. |
| `correlation-id` | Links the event to the gRPC call that caused it. |

`events.Decode` looks the type up in protobuf's registry, so adding a new event type needs no change there. Unknown fields are ignored, so a producer can add fields before consumers are updated.

Consumers **ignore event types they do not handle** (the `default: return nil` branches). That lets a producer add new event types to a topic without breaking anyone.

## The producer: `pkg/kafka/producer.go`

```go
RequiredAcks: kafkago.RequireAll      // wait until all in-sync replicas have the message
Balancer:     &kafkago.Murmur2Balancer{}   // same key→partition mapping as the Java client
```

`acks=all` is the durable setting: slower than `acks=1` but an acknowledged message survives the loss of a broker. Services never call the producer directly for business events; they write to the **outbox** and the relay publishes ([doc 4](04-postgres-and-outbox.md)).

## The consumer: `pkg/kafka/consumer.go`

This is the most instructive file in the repository. It solves four problems.

```
                    ┌── worker 0 ──┐
Kafka ─► fetch ────►├── worker 1 ──┤──► handler ──► mark done
         loop       └── worker N ──┘                    │
          ▲     route by hash(key)                      ▼
          │                                  offset tracker
          └────── backpressure                         │
                                              commit loop (every 1s)
```

### Problem 1: process in parallel but keep order per key

One goroutine handling messages one at a time is correct but slow. A plain worker pool is fast but could run two events of the same order at the same time, in any order.

Solution: a **keyed worker pool** (`pkg/workerpool`). Each worker has its own queue; a message goes to worker `hash(key) % N`. Same order ID → same worker → sequential. Different orders → different workers → parallel.

### Problem 2: which offset is safe to commit?

With parallel workers, messages finish out of order. Suppose offsets 5, 6, 7 were fetched and 6 and 7 finished but 5 is still running. Committing 7 and then crashing would lose message 5 forever.

Solution: the **offset tracker** (`pkg/kafka/offsets.go`). It remembers the fetch order and only lets the commit position advance over a *contiguous* run of finished messages:

```
fetched:  5  6  7  8
done:     .  ✓  ✓  .     → nothing to commit (5 is still running)
done:     ✓  ✓  ✓  .     → commit 7
```

Read `offsets_test.go`; the last test checks this property for random completion orders.

### Problem 3: a message fails

`process()` wraps the handler in `retry.Do`:

- **Transient error** (database down, gateway timeout): retry with exponential backoff and jitter.
- **Permanent error** (`retry.Permanent(err)`: malformed JSON, unknown order): retrying cannot help, skip the retries.
- **Out of retries or permanent**: publish the message to the **dead-letter queue** (`<service>.dlq`) with headers saying where it came from and why it failed, then move on.

Without a DLQ, one unprocessable message (a "poison pill") blocks its partition forever, because the consumer can never commit past it.

Each handler classifies its own errors. `internal/order/eventhandler/handler.go` is a good example:

```go
case errors.Is(err, domain.ErrAlreadyProcessed):  return nil                   // duplicate: fine
case errors.Is(err, domain.ErrInvalidTransition): return nil                   // business situation, not a failure
case errors.Is(err, domain.ErrNotFound):          return retry.Permanent(err)  // DLQ
default:                                          return err                   // retry
```

A trade-off to know about: retrying *in place* delays the other messages queued on the same worker. For long outages, larger systems move failed messages to separate retry topics with increasing delays.

### Problem 4: do not fetch faster than you can process

The queues between the fetch loop and the workers are bounded. When workers are busy, `pool.Submit` blocks, the loop stops fetching, and the messages simply wait in Kafka. This is **backpressure**: memory use stays flat no matter how large the backlog.

### Problem 5: Kafka itself is unavailable

A broker outage must not take the service down: order-service can still accept orders with only Postgres. The fetch loop retries failed fetches forever with a capped backoff, and the outbox relay backs off the same way while events wait safely in the outbox table. When Kafka returns, both recover on their own (experiment 2 in [doc 7](07-running.md)). The metrics `kafka_consumer_fetch_errors_total` and `outbox_publish_errors_total` make the outage visible.

### Shutdown

`Run()` on shutdown: stop fetching → let workers finish what they hold → commit the final offsets → close the reader (which leaves the group so the partitions are reassigned immediately). Handlers run with a context that is *not* cancelled at shutdown, so in-flight work completes instead of failing halfway.

## Limits of this implementation (honest notes)

- **Rebalances.** If a partition is taken away while messages from it are in flight, they finish here *and* are processed again by the new owner. Correct (handlers are idempotent) but duplicated work. Production clients hook into rebalance callbacks to stop and commit first.
- **Schema registry.** Schemas here are compiled into each service. Larger organizations store schemas in a registry that rejects incompatible changes at publish time.
- **Kafka transactions** (the "exactly-once semantics" feature) are not used; the outbox + idempotent-consumer approach gives the same end result and also covers the database.

## Try it

1. Open Kafka UI (http://localhost:18080) → Topics → `orders.v1` → Messages. Create an order and read the event, its key and headers.
2. Kafka UI → Consumers. Run `shopctl load -n 2000 -c 50` and watch the lag rise and fall.
3. **Catch-up**: `docker compose stop payment-service`, create three orders (they stay `PENDING`), then `docker compose start payment-service` and watch them get paid.
4. **Scale out and watch a rebalance**: with the Docker stack running, start a second notification-service on your machine: `OPS_ADDR=:18093 go run ./cmd/notification-service` (PowerShell: `$env:OPS_ADDR=":18093"; go run ./cmd/notification-service`). In Kafka UI → Consumers → `notification-service` you now see two members sharing the partitions. Stop it with Ctrl+C and the partitions move back.
5. **Dead-letter queue**: in Kafka UI, produce a message to `orders.v1` with value `not json` and header `event-type: orderflow.events.v1.OrderCreated`. Find it in `payment-service.dlq` and read its `x-error` header.

Next: [Postgres and the outbox](04-postgres-and-outbox.md)
