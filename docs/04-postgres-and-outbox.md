# 4. Postgres, the outbox and idempotency

## Why Postgres here (and when MongoDB)

The core of this system is "change the order **and** record an event, atomically". That is a multi-row, multi-table transaction, which is what a relational database is best at. Constraints (`UNIQUE`, `CHECK`, foreign keys) also act as a last line of defence that application bugs cannot bypass.

MongoDB fits better when the data is document-shaped, the schema varies a lot between records, or you need to scale writes horizontally by sharding. If you swapped it in, only `internal/order/repository/` would change: a new type implementing `service.Repository`. (MongoDB has multi-document transactions on replica sets, so the outbox pattern works there too.)

## Schema: `internal/order/repository/migrations/0001_init.sql`

Read the file; the comments explain each decision. Highlights:

- `CHECK (status IN (...))`, `CHECK (total_minor >= 0)`: the database refuses invalid data even if the Go code has a bug.
- `UNIQUE (customer_id, idempotency_key)`: makes order creation idempotent.
- `CREATE INDEX ... (customer_id, created_at DESC, id DESC)`: built for exactly the `ListOrders` query.
- `CREATE INDEX ... ON outbox (id) WHERE published_at IS NULL`: a **partial index** containing only unpublished rows, so it stays tiny however large the table grows.

### Migrations

Schema changes are numbered SQL files compiled into the binary (`//go:embed`). On start, `postgres.Migrate` (`pkg/postgres/postgres.go`) applies the ones not yet recorded in `schema_migrations`. It takes a Postgres **advisory lock** first, so when three replicas start at once only one runs the migrations.

Production teams normally use a tool such as `goose` or `golang-migrate` and often run migrations as a separate deploy step rather than at service start. The mechanism is the same.

## Connection pool: `postgres.Connect`

Opening a database connection is slow, so `pgxpool` keeps a set open and lends them to goroutines. `MaxConns` matters: total connections = replicas × `MaxConns` and must stay under the server's `max_connections`. When all are in use, the next query waits, which is another form of backpressure.

## Transactions: `postgres.InTx`

```go
err := postgres.InTx(ctx, pool, func(tx pgx.Tx) error {
    // everything here commits together, or not at all
})
```

The helper commits if the function returns `nil` and rolls back on an error or a panic. It removes the classic bug of forgetting the rollback on one of several error paths.

**Rule: never do slow network calls inside a transaction.** An open transaction holds a connection and row locks. `internal/payment/handler.go` calls the payment gateway *first* and opens the transaction only to record the result.

## The dual-write problem and the outbox

A service must do two things: save the order and publish `OrderCreated`. They are two different systems; no transaction covers both.

```
save order ✓ → publish ✗ (Kafka down, or crash)   → order exists, nobody is told. It stays PENDING forever.
publish ✓ → save order ✗ (DB error)               → a payment is taken for an order that does not exist.
```

No ordering of the two steps fixes this. The fix is to make it **one** write:

```
┌──────────── one Postgres transaction ────────────┐
│ INSERT INTO orders ...                            │
│ INSERT INTO order_items ...                       │
│ INSERT INTO outbox (topic, key, payload, ...)     │   ← the event is just another row
└───────────────────────────────────────────────────┘

relay goroutine, forever:
    SELECT ... FROM outbox WHERE published_at IS NULL ... FOR UPDATE SKIP LOCKED
    publish to Kafka
    UPDATE outbox SET published_at = now()
```

Code: `outbox.Insert` is called inside the repository's transaction (`repository/postgres.go`, `Create` and `Update`); the relay is `pkg/outbox/outbox.go`.

Either the order and its event both exist, or neither does. If Kafka is down, events wait in the table and are published when it returns.

Details in the relay:

- **`FOR UPDATE SKIP LOCKED`** locks the selected rows and *skips* rows locked by someone else. Two replicas running the relay take different rows instead of blocking each other. This clause is how you build a job queue on Postgres.
- **It can publish twice.** If the relay crashes after publishing but before the `UPDATE` commits, the rows are published again next time. The outbox gives **at-least-once**, which is why events have an `event_id` and consumers de-duplicate.
- **Draining.** While batches come back full the relay loops immediately rather than waiting for the next tick.
- **Cleanup.** Published rows older than 7 days are deleted hourly.

Polling every 200 ms adds a small delay. The lower-latency alternative is **change data capture** (e.g. Debezium), which reads the database's write-ahead log and pushes outbox rows to Kafka as they commit.

One caveat: with several relay replicas, `SKIP LOCKED` can publish two events of the same order out of order. If strict per-key order matters, run a single relay (leader election) or use CDC.

## Idempotency, three ways

At-least-once delivery means everything can happen twice. Each place where that matters has a guard.

### 1. Client retries → idempotency key

A client that times out on `CreateOrder` retries with the same `idempotency_key`.

```sql
INSERT INTO orders (...) VALUES (...)
ON CONFLICT ON CONSTRAINT orders_idempotency_uniq DO NOTHING
```

If the insert affected 0 rows, the order already exists: return it, write no event. The unique constraint makes this safe even when two retries arrive at the same instant on different replicas; a "check, then insert" in Go would have a race. The integration test `TestConcurrentIdempotentCreate` fires 20 at once and asserts exactly one order.

### 2. Kafka redelivery → the inbox table

`repository.Update`, when called from a consumer:

```sql
BEGIN;
INSERT INTO inbox (consumer, event_id) VALUES (...) ON CONFLICT DO NOTHING;   -- 0 rows? already processed, stop
SELECT ... FROM orders WHERE id = $1 FOR UPDATE;
UPDATE orders SET status = 'PAID', ...;
COMMIT;
```

The "I have seen this event" record commits in the **same transaction** as the effect of the event. They cannot disagree: if the transaction fails, both are rolled back and the event will be retried.

### 3. External side effects → idempotency key at the provider

Charging a card is not in our database, so a transaction cannot protect it. `payment/handler.go` documents three layers:

1. If a payment row for the order exists, skip.
2. The gateway call carries `IdempotencyKey = order ID`. A repeated call returns the first result instead of charging again (real providers offer this; `FakeGateway` simulates it).
3. `UNIQUE (order_id)` on `payments` plus the inbox.

The dangerous moment is a crash *after* the gateway charged and *before* our transaction committed. On redelivery, layer 1 finds nothing, layer 2 returns the original result without a second charge, and the result is recorded. `TestRetryAfterDBFailureIsSafe` reproduces it.

The notification-service uses a cheaper guard: an in-memory LRU of seen event IDs. It is lost on restart, so a duplicate email is possible after a crash. That is an explicit trade-off: a duplicate email is tolerable, a duplicate charge is not. Match the strength of the guard to the cost of the duplicate.

## Locking: `SELECT ... FOR UPDATE`

`repository.Update` reads the order with `FOR UPDATE`, which locks the row until commit. A second transaction updating the same order waits, then sees the new state, and the state machine rejects it if it no longer applies. `TestConcurrentUpdatesAreSerialized` races `PAID` against `PAYMENT_FAILED` and asserts that exactly one wins.

This is **pessimistic locking**. The alternative, **optimistic locking**, takes no lock and writes `UPDATE ... WHERE id = $1 AND version = $2`; if 0 rows change, someone else got there first and you retry. Optimistic is better when conflicts are rare and transactions are long. The `version` column exists so clients can see changes and so you can try this as an exercise.

## Reading efficiently

### Avoid N+1 queries

`loadItems` fetches the items of *all* listed orders with one query (`WHERE order_id = ANY($1)`). Loading 100 orders costs 2 queries, not 101.

### Keyset pagination

```sql
-- OFFSET: the database reads and throws away 100 000 rows
SELECT ... ORDER BY created_at DESC LIMIT 20 OFFSET 100000;

-- Keyset: jumps straight to the position using the index
SELECT ... WHERE customer_id = $1 AND (created_at, id) < ($2, $3)
ORDER BY created_at DESC, id DESC LIMIT 20;
```

The client receives an opaque `next_page_token` (base64 of the last row's `created_at` and `id`). Every page costs the same, and rows inserted while paging do not cause skips or duplicates. `id` is in the ordering as a tie-breaker because two orders can share a timestamp.

### Batching

Order items are inserted with `pgx.Batch`: all statements go to the server in one network round trip.

## Try it

Connect: `docker compose exec postgres psql -U orderflow -d orders`

1. Create an order, then: `SELECT id, topic, event_type, published_at FROM outbox ORDER BY id DESC LIMIT 5;`
2. **See the outbox hold events**: `docker compose stop kafka`, create two orders (it works: only Postgres is needed), look at the outbox (`published_at` is NULL), `docker compose start kafka`, and watch them publish and get paid.
3. `SELECT * FROM inbox;` after an order is paid.
4. `EXPLAIN ANALYZE` the keyset query from `repository/postgres.go` and find the index in the plan.
5. Exercise: change `Update` to optimistic locking using `version`, and make `TestConcurrentUpdatesAreSerialized` pass.

Next: [Concurrency and data structures](05-concurrency.md)
