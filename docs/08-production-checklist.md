# 8. Production checklist

What this project already does the way a production system would, and what a real deployment adds on top. Use the second half as a list of things to learn next.

## Already here

**API**
- [x] Contract-first API in protobuf, versioned packages (`v1`)
- [x] Input validation with limits; overflow-safe money arithmetic in integer minor units
- [x] Correct gRPC status codes and machine-readable error details
- [x] Idempotent create via idempotency key
- [x] Cursor (keyset) pagination with opaque tokens
- [x] Interceptors: request ID, logging, metrics, panic recovery
- [x] gRPC health service, keepalive, connection max age, message size limit
- [x] Deadlines and client retry policy

**Messaging**
- [x] Transactional outbox (no dual writes)
- [x] Idempotent consumers (inbox table / provider idempotency key)
- [x] Partition key chosen for ordering; per-key ordering kept under concurrency
- [x] Offsets committed only after processing, contiguous-watermark commits
- [x] Retries with exponential backoff and jitter; permanent vs transient errors
- [x] Dead-letter queue per consumer with diagnostic headers
- [x] Backpressure from workers to the fetch loop
- [x] Explicit topic creation; `acks=all`
- [x] Tolerant readers: unknown fields and unknown event types are ignored

**Data**
- [x] Database per service
- [x] Versioned migrations guarded by an advisory lock
- [x] Constraints as a safety net (`CHECK`, `UNIQUE`, foreign keys)
- [x] Connection pooling with limits; no network calls inside transactions
- [x] Row locking for concurrent updates; no N+1 queries

**Operations**
- [x] Configuration from environment, validated at startup
- [x] Structured JSON logs with a correlation ID across gRPC and Kafka
- [x] Prometheus metrics; liveness and readiness endpoints
- [x] Graceful shutdown in dependency order, with timeouts
- [x] Small non-root distroless images, multi-stage builds
- [x] CI: format, vet, race-enabled tests, integration tests, image builds

## What a real deployment adds

### Security (the biggest gap; do these first)
- **TLS everywhere.** gRPC uses `insecure.NewCredentials()` here. Production uses TLS, often mutual TLS between services (frequently provided by a service mesh such as Istio or Linkerd).
- **Authentication and authorization.** Anyone who can reach port 50061 can read or cancel any order. Add an interceptor that validates a JWT/OIDC token and check that the caller owns the order.
- **Kafka security.** SASL authentication, TLS, and ACLs so each service can only read and write its own topics.
- **Secrets.** Database passwords come from a secret manager (Vault, AWS Secrets Manager, Kubernetes Secrets), never from a compose file or the repository.
- **Reflection off** in production, and rate limiting per client at the edge.

### Reliability
- **Kafka cluster**: 3+ brokers, replication factor 3, `min.insync.replicas=2`. The single broker here loses data if its disk dies.
- **Postgres**: a replica with automatic failover, backups, and tested restores.
- **Rebalance handling** in the consumer: commit and stop work for revoked partitions (see the note in [doc 3](03-kafka.md)).
- **Retry topics** with delays, so one failing message does not hold up its neighbours.
- **DLQ tooling**: alert when the DLQ is not empty, and a command to replay messages after a fix.
- **Circuit breaker** around the payment gateway, to stop calling a provider that is clearly down.
- **Timeouts and stuck-order detection**: a job that finds orders `PENDING` for too long and alerts or cancels them. Every saga needs an answer to "what if the reply never comes?".
- **Outbox ordering** with multiple relay instances (leader election or CDC with Debezium).

### Observability
- **Distributed tracing** with OpenTelemetry. The correlation ID here gives you log joining; tracing adds timing for every hop, shown as a waterfall (Jaeger, Tempo).
- **Dashboards and alerts** (Grafana + Alertmanager): error rate, latency percentiles, consumer lag, DLQ size, outbox backlog, DB pool saturation.
- **SLOs**, for example "99.9% of `CreateOrder` calls succeed within 300 ms", and alerting on the error budget.

### Contracts and evolution
- **Buf** for linting protos and detecting breaking changes in CI.
- **Schema registry** for events, enforcing compatibility when publishing.
- **Consumer-driven contract tests** between services.

### Deployment
- **Kubernetes** manifests or Helm charts: liveness/readiness probes wired to `/healthz` and `/readyz`, resource limits, a `terminationGracePeriodSeconds` longer than `SHUTDOWN_TIMEOUT`, a PodDisruptionBudget.
- **Autoscaling** payment-service on consumer lag (KEDA).
- **Migrations as a deploy step**, and the expand/contract technique for schema changes without downtime.
- **Progressive delivery**: canary or blue-green releases.
- **Image scanning and dependency updates** (`govulncheck`, Dependabot/Renovate).

### Product gaps (deliberately out of scope)
- An HTTP/JSON gateway for browsers (grpc-gateway or Connect).
- Inventory reservation as a third saga participant.
- A real payment provider and a real email provider.
- A shared cache (Redis) so reads and `WatchOrder` work across replicas.

## Suggested next steps for learning

In rough order of value:

1. Add a **JWT auth interceptor** and an ownership check in `GetOrder`.
2. Add **OpenTelemetry tracing** across gRPC and Kafka; view a trace in Jaeger.
3. Add an **inventory-service** that reserves stock on `OrderCreated` and releases it on `OrderCancelled` or `PaymentFailed`.
4. Add a **stuck-order reaper** goroutine in order-service.
5. Replace the in-process `watch.Broker` with **Redis pub/sub** and run two order-service replicas.
6. Write a **DLQ replay** subcommand in `shopctl`.
7. Implement `service.Repository` on **MongoDB** and run the same integration tests against it.
8. Deploy to a local **Kubernetes** cluster (kind or minikube).
