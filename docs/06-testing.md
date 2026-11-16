# 6. Testing

## The pyramid

| Level | Command | Needs | Speed | Proves |
|---|---|---|---|---|
| **Unit** | `go test ./...` | nothing | seconds | Business rules, algorithms, error handling |
| **Integration** | `go test -tags=integration ./test/integration/...` | Postgres | seconds | Our SQL, constraints and locking really work |
| **End-to-end** | `go test -tags=e2e ./test/e2e/...` | the whole stack | seconds–minutes | The services work *together* |

Many unit tests, fewer integration tests, a handful of end-to-end tests. Lower levels are faster and point at the exact cause of a failure; higher levels catch what the lower ones cannot see (a wrong SQL statement, a mismatched topic name).

Integration and e2e tests carry a **build tag** (`//go:build integration` on the first line), so a plain `go test ./...` never requires Docker.

## Unit tests

### Table-driven tests

The standard Go style: a slice of cases, one loop, `t.Run` for each. Adding a case is one line. See `TestNewOrderValidation` in `internal/order/domain/order_test.go`:

```go
tests := []struct{ name, customer, currency string; items []Item; wantField string }{
    {"empty customer", "", "USD", validItems(), "customer_id"},
    {"lowercase currency", "c", "usd", validItems(), "currency"},
    ...
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) { ... })
}
```

### Testing a state machine exhaustively

`TestStateMachine` loops over **every** (from, to) pair of statuses and checks that allowed transitions succeed and all others are rejected *without changing the order*. Four states make sixteen pairs; a loop covers them all, including the ones nobody would think to write by hand.

### Fakes instead of mocks

The service layer needs a repository. Instead of a mocking library that scripts each call, the tests use a **fake**: a small, working, in-memory implementation (`internal/order/service/memrepo.go`).

A fake behaves like the real thing (idempotency keys, inbox, "rollback" on error), so tests assert on *outcomes* ("the order is PAID and one event was emitted") rather than on *which methods were called in which order*. Such tests survive refactoring.

This works because the service depends on an interface it defines itself (`service.Repository`).

### Controlling time and randomness

Code that calls `time.Now()` directly is hard to test. Types here hold a `now func() time.Time` field that tests replace:

- `lru_test.go` advances a fake clock to test TTL expiry instantly.
- `tokenbucket_test.go` checks refill arithmetic without waiting.
- `service_test.go` uses an increasing clock so "newest first" ordering is deterministic.

### Testing gRPC for real: `bufconn`

`internal/order/grpcapi/server_test.go` starts a real gRPC server on an **in-memory connection**. Requests go through protobuf encoding, the interceptor chain and status mapping exactly as in production, with no TCP port. It tests things a direct method call cannot: status codes, error details, response headers, and the streaming RPC (including the stream ending with `io.EOF` and the client cancelling mid-stream).

### Testing concurrent code without `time.Sleep`

Sleeping and hoping makes tests slow and flaky. Techniques used instead:

- **Wait for the work to finish**: `pool.Close()` returns only when every task ran, then assert.
- **Gates**: `TestSchedulerSendsHighPriorityFirst` uses a fake rate limiter that blocks until the test closes a channel. The test queues jobs in a known state, opens the gate, and asserts the exact send order.
- **Poll a condition with a deadline** (`waitFor`) when you must wait for another goroutine.
- **Property-style tests**: `TestRandomCompletionOrder` (`pkg/kafka/offsets_test.go`) completes offsets in random order 50 times and asserts an invariant: *nothing is ever committed past an unfinished message*.
- **The race detector**: `go test -race`.

### Testing failure paths

The interesting bugs are in the unhappy paths, so they are tested deliberately:

| Test | Scenario |
|---|---|
| `TestRedeliveryDoesNotChargeTwice` | Kafka delivers the same message three times |
| `TestRetryAfterDBFailureIsSafe` | Crash between "gateway charged" and "result saved" |
| `TestGatewayOutageIsTransient` | Provider down → retryable error, no event emitted |
| `TestErrorClassification` | Each error maps to ack / retry / DLQ |
| `TestFailedSendIsRetriedNotDeduplicated` | A failed send must not be remembered as done |
| `TestPublishNeverBlocksOnSlowSubscriber` | A stuck stream client cannot stall the publisher |

## Integration tests: `test/integration/repository_test.go`

They run the real Postgres adapter against a real database (`orders_test`, separate from development data).

- `TestMain` connects and migrates once for the package.
- Each test uses a **unique customer ID**, so tests are independent and can run in any order without wiping tables.
- They cover what only a real database can show:
  - `TestConcurrentIdempotentCreate`: 20 goroutines, one idempotency key, exactly one order.
  - `TestConcurrentUpdatesAreSerialized`: two competing transitions, exactly one wins (row lock).
  - `TestUpdateIsAtomicWithInboxAndOutbox`: a failed update rolls back its inbox row too.
  - `TestOutboxRelayPublishesAndMarksRows`: the relay with a fake publisher.

A popular alternative is [testcontainers-go](https://golang.testcontainers.org/), which starts a throwaway Postgres container from inside the test.

## End-to-end tests: `test/e2e/flow_test.go`

Only the public API is used, as a real client would:

- `TestOrderIsPaid`: create → follow the stream → `PAID`. Passes only if gRPC, Postgres, the outbox, Kafka and both consumers all work.
- `TestExpensiveOrderFailsPayment`: the decline path.
- `TestNotificationsAreSent`: polls the notification debug endpoint until both notifications appear.
- `TestCancelPaidOrder`: cancellation.

Because the system is asynchronous, e2e tests **wait for an outcome with a timeout**; they never sleep a fixed time and assume it was long enough.

## Useful commands

```bash
go test ./...                                    # everything (unit)
go test ./internal/order/... -run StateMachine -v   # one test, verbose
go test -count=1 ./...                           # bypass the test cache
go test -race ./...                              # race detector
go test -cover ./...                             # coverage per package
go test -coverprofile=c.out ./... && go tool cover -html=c.out   # coverage in the browser
go test -bench=. -benchmem ./pkg/ds/lru/         # benchmarks
```

## CI: `.github/workflows/ci.yml`

Every push and pull request runs: format check, `go vet`, unit tests with the race detector and coverage, integration tests against a Postgres service container, and a Docker build of each service.

Next: [Running and experiments](07-running.md)
