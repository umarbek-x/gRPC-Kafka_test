# 5. Concurrency and data structures

## Goroutines in this system

A goroutine is a function running concurrently with the rest of the program. They are cheap (a few kilobytes), so Go programs use many. A running order-service has:

| Goroutine(s) | Started in | Job |
|---|---|---|
| gRPC server | `main.go` → `grpcServer.Serve` | Accepts connections; gRPC then starts **one goroutine per request**. |
| Outbox relay | `outbox.Relay.Run` | Loop on a ticker: publish pending events. |
| Kafka fetch loop | `kafka.Consumer.Run` | Reads messages, hands them to workers. |
| N workers | `workerpool.NewKeyed` | Run the event handlers. |
| Offset commit loop | `kafka.Consumer.commitLoop` | Commits offsets every second. |
| Ops HTTP server | `observability.OpsServer.Run` | `/healthz`, `/readyz`, `/metrics`. |
| One per `WatchOrder` stream | gRPC | Lives as long as the client stays connected. |

**The rule for every goroutine you start: know how it stops.** A goroutine that waits forever on something that never happens is a *leak*; enough leaks and the process runs out of memory. In this codebase every loop selects on `ctx.Done()`.

## Context: cancellation and deadlines

`context.Context` is the first parameter of almost every function here. It carries three things down the call chain:

1. **Cancellation.** When the parent is cancelled, everything below stops. One Ctrl+C cancels the root context and every loop in the process exits.
2. **Deadlines.** The client's 5-second deadline reaches the gRPC handler, and because the handler passes the same `ctx` to `pool.Query`, the SQL query is cancelled when the client gives up.
3. **Request-scoped values**, used sparingly: here only the correlation ID.

The standard way to wait for something while staying cancellable:

```go
select {
case <-ctx.Done():
    return ctx.Err()
case <-timer.C:
}
```

You will find this shape in `retry.Do`, `TokenBucket.Wait`, `FakeGateway.simulateNetwork` and `Scheduler.Submit`. A bare `time.Sleep` cannot be interrupted, so it is avoided.

One subtle use: `kafka.Consumer.Run` gives handlers `context.WithoutCancel(ctx)`. On shutdown we want to stop *fetching*, but let handlers already running *finish*; cancelling them halfway would only cause retries.

## Channels and their patterns

A channel passes values between goroutines safely.

| Pattern | Where | How it works |
|---|---|---|
| **Work queue** | `workerpool` | Producers send tasks into a buffered channel; workers `range` over it. |
| **Backpressure** | `Pool.Submit` | Sending to a full channel blocks, which slows the sender down. |
| **Graceful drain** | `Pool.Close` | `close(ch)`: `range` keeps delivering what is buffered, then ends. |
| **Done signal** | `commitDone`, `schedDone` | `close(done)` wakes everyone waiting on `<-done`. |
| **Result of one job** | `notification.job.result` | A channel of capacity 1 so the sender never blocks, even if the receiver left. |
| **Wake-up** | `Scheduler.wake` | A non-blocking send to a capacity-1 channel: "there is work". |
| **Fan-out** | `watch.Broker.Publish` | One update is sent to every subscriber's own channel. |

Two rules that prevent most channel bugs: **only the sender closes a channel**, and **sending on a closed channel panics**. `Pool.Submit`/`Pool.Close` show how to make that safe: an `RWMutex` lets many submits proceed together but makes `Close` wait for them.

## Mutexes and atomics

Use a channel to *hand work over*; use a mutex to *protect shared state*. `lru.Cache`, `offsetTracker`, `watch.Broker` and `TokenBucket` all guard their fields with a `sync.Mutex`. `RWMutex` (many readers or one writer) is used where reads dominate. For single counters, `sync/atomic` is enough (`shopctl load`).

Go's **race detector** finds unsynchronized access at runtime: `go test -race ./...`. CI runs it on every push. (On Windows it needs a C compiler; in WSL or CI it works out of the box.)

## Worker pools: `pkg/workerpool/pool.go`

Starting one goroutine per task is easy but **unbounded**: 100 000 tasks become 100 000 concurrent database queries. A pool caps concurrency at N and holds at most `queueSize` waiting tasks.

### `Pool` — N workers, one shared queue

```
Submit ──► [ queue ] ──► worker 0
                    ├──► worker 1
                    └──► worker 2
```

Any worker takes any task. Used by `shopctl load` to fire requests with a fixed concurrency.

### `KeyedPool` — N workers, one queue each

```
Submit(key) ── hash(key) % N ──► [queue 0] ─► worker 0
                               ├► [queue 1] ─► worker 1
                               └► [queue 2] ─► worker 2
```

Tasks with the same key always go to the same worker and so run **in order**. Different keys run in parallel. This is what makes concurrent Kafka consumption safe ([doc 3](03-kafka.md)).

Both pools:

- **Recover from panics** so one bad task does not kill a worker.
- **Block in `Submit` when full** and honour the context while waiting.
- **Drain on `Close`**: queued tasks finish before `Close` returns.

Choosing N: for I/O-bound work (database, network) N can be much larger than the number of CPU cores, limited by what the downstream system can take (for example the DB pool size). For CPU-bound work, N near the core count.

## A group of goroutines as one unit: `pkg/lifecycle/group.go`

A service is healthy only if *all* its loops run. `lifecycle.Group`:

- starts each component in a goroutine;
- cancels the shared context if **any** component returns an error (a half-working service is worse than a restart);
- cancels on SIGINT/SIGTERM;
- waits for everything to stop, with a timeout.

### Shutdown order

Stop the *sources* of work before the things they depend on:

1. Report not-ready, so the load balancer stops sending traffic.
2. gRPC `GracefulStop`: finish in-flight calls.
3. Consumer: stop fetching, drain workers, commit offsets.
4. Close the Kafka producer and the DB pool (deferred, so last).

`cmd/notification-service/main.go` has an explicit example: the scheduler has its own context and is stopped only *after* the consumer has drained, because the consumer's workers are waiting on the scheduler.

## Data structures

Each one is small, generic (`[T any]`), tested, and used for a real purpose.

### LRU cache — `pkg/ds/lru`

**Hash map + doubly linked list.** The map finds an entry in O(1). The list keeps entries in order of use: a hit moves the entry to the front, and when the cache is full the entry at the back (least recently used) is evicted. Every operation is O(1).

Used for: the order read cache (`service.GetOrder`, cache-aside pattern), the gateway's idempotency results, the notification de-duplication set.

Know its limit: the cache is per process. With several replicas, one replica can serve a value up to `ORDER_CACHE_TTL` old after another replica changed the order. A shared cache (Redis) or event-based invalidation solves that.

### Priority queue — `pkg/ds/pqueue`

**Binary heap in a slice.** The best item is always at index 0; push and pop are O(log n). Used by the notification scheduler so that "payment failed" is sent before "we received your order". Ties are broken by arrival number, giving FIFO within a priority.

### Ring buffer — `pkg/ds/ringbuffer`

**Fixed slice + a write index that wraps around.** Keeps the last N items in constant memory. Backs `GET /debug/notifications`.

### Token bucket — `pkg/ratelimit`

A bucket holds up to `burst` tokens and refills at `rate` per second; each send takes one. It allows short bursts but enforces the average. Refilling is computed lazily from elapsed time, so there is no background goroutine.

### Offset tracker — `pkg/kafka/offsets.go`

**FIFO slice + set.** Finds the highest offset below which everything is finished ([doc 3](03-kafka.md)).

### Pub/sub broker — `internal/order/watch`

**Map of maps of channels**: order ID → subscriber ID → channel. `Publish` uses a non-blocking send: if a subscriber's buffer is full the update is dropped *for that subscriber*, so one slow client can never stall the Kafka consumer that publishes.

## Putting them together: the notification scheduler

`internal/notification/scheduler.go` combines a mutex-protected heap, a wake-up channel, a rate limiter and several dispatcher goroutines:

```
Kafka workers ── Submit ──► [priority queue] ──► dispatchers ──► limiter ──► Sender
       ▲                                                                       │
       └──────────────────────── result ◄───────────────────────────────────────┘
```

`Submit` **blocks until the notification is sent**. That is deliberate: the Kafka offset is committed only when the handler returns, so a notification still sitting in the in-memory queue has not been acknowledged. If the process dies, Kafka redelivers it. In-memory queue, no data loss.

Read `pop()` for a classic concurrency bug and its fix, the **lost wake-up**: with a capacity-1 signal channel, two quick pushes can produce a single signal; the dispatcher that takes a job passes the signal on if more jobs remain.

## Try it

1. `go test ./pkg/workerpool/ -run KeyedPool -v` and read the test: it proves per-key ordering.
2. In `pkg/ds/lru/lru.go`, delete the `c.mu.Lock()` in `Get`, then run `go test -race ./pkg/ds/lru/` (WSL/Linux/CI) and read the race report.
3. Set `SEND_RATE_PER_SECOND=2` on notification-service, run `shopctl load -n 100`, and watch `/debug/notifications`: failures (priority 9) arrive before receipts (priority 1), and notification lag grows in Kafka UI.
4. Set `CONSUMER_WORKERS=1` on payment-service and compare `shopctl load` throughput with `CONSUMER_WORKERS=32`.
5. Exercise: add `TrySubmit(task) bool` to `Pool` that returns `false` instead of blocking when the queue is full (load shedding), with a test.

Next: [Testing](06-testing.md)
