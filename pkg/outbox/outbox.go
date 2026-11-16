// Package outbox implements the Transactional Outbox pattern.
//
// THE PROBLEM ("dual write"): a service must both update its database AND
// publish an event. These are two different systems; there is no transaction
// spanning Postgres and Kafka.
//
//	save order ✓ → publish event ✗ (Kafka down)   => other services never learn about the order
//	publish event ✓ → save order ✗ (DB error)     => others process an order that doesn't exist
//
// THE SOLUTION: write the event into an `outbox` table IN THE SAME DATABASE
// TRANSACTION as the business change. Either both are committed or neither.
// A background Relay then reads unpublished rows, publishes them to Kafka and
// marks them as published.
//
//	┌──────────── one DB transaction ────────────┐
//	│ INSERT INTO orders ...                      │
//	│ INSERT INTO outbox (topic, key, payload)    │
//	└─────────────────────────────────────────────┘
//	           Relay (goroutine): SELECT unpublished → Kafka → UPDATE published_at
//
// Guarantee: at-least-once publishing. If the relay crashes after publishing
// but before marking the row, the event is published again. That is why every
// event carries a unique event_id and consumers de-duplicate.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/postgres"
)

// Message is one event waiting to be published.
type Message struct {
	EventID   string
	Topic     string
	Key       string
	EventType string
	Payload   []byte
	Headers   map[string]string
}

// Insert stores messages using the caller's transaction. This is the whole
// point: the outbox row commits atomically with the business data.
func Insert(ctx context.Context, tx pgx.Tx, msgs ...Message) error {
	for _, m := range msgs {
		h, err := json.Marshal(m.Headers)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO outbox (event_id, topic, msg_key, event_type, payload, headers)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			m.EventID, m.Topic, m.Key, m.EventType, m.Payload, h); err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}
	return nil
}

// Publisher is satisfied by *kafka.Producer (and by fakes in tests).
type Publisher interface {
	Publish(ctx context.Context, msgs ...kafka.Message) error
}

type RelayConfig struct {
	PollInterval time.Duration // how often to look for new rows when idle
	BatchSize    int
	Retention    time.Duration // delete published rows older than this
}

var (
	metricPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_published_total", Help: "Outbox messages published to Kafka.",
	})
	metricErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_publish_errors_total", Help: "Failed outbox relay batches.",
	})
)

// Relay moves outbox rows to Kafka.
type Relay struct {
	pool *pgxpool.Pool
	pub  Publisher
	cfg  RelayConfig
	log  *slog.Logger
}

func NewRelay(pool *pgxpool.Pool, pub Publisher, cfg RelayConfig, log *slog.Logger) *Relay {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Retention <= 0 {
		cfg.Retention = 7 * 24 * time.Hour
	}
	return &Relay{pool: pool, pub: pub, cfg: cfg, log: log.With("component", "outbox-relay")}
}

// Run polls until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	r.log.Info("outbox relay started", "poll_interval", r.cfg.PollInterval)
	poll := time.NewTicker(r.cfg.PollInterval)
	cleanup := time.NewTicker(time.Hour)
	defer poll.Stop()
	defer cleanup.Stop()

	// While Kafka is down every attempt fails. Back off (up to 5s) instead of
	// hammering the broker and flooding the logs every poll interval.
	var (
		failures  int
		notBefore time.Time
	)
	for {
		select {
		case <-ctx.Done():
			r.log.Info("outbox relay stopped")
			return nil
		case <-cleanup.C:
			r.cleanup(ctx)
		case <-poll.C:
			if time.Now().Before(notBefore) {
				continue
			}
			// Drain: keep going while full batches come back, so a backlog
			// is cleared quickly instead of one batch per tick.
			for {
				n, err := r.publishBatch(ctx)
				if err != nil {
					metricErrors.Inc()
					failures++
					wait := min(r.cfg.PollInterval<<min(failures, 10), 5*time.Second)
					notBefore = time.Now().Add(wait)
					if ctx.Err() == nil {
						r.log.Warn("outbox publish failed; events stay queued", "failures", failures, "retry_in", wait.String(), "err", err)
					}
					break
				}
				if failures > 0 {
					r.log.Info("outbox publishing recovered", "after_failures", failures)
					failures = 0
				}
				if n < r.cfg.BatchSize {
					break
				}
			}
		}
	}
}

// publishBatch locks a batch of rows, publishes them, and marks them.
//
// FOR UPDATE SKIP LOCKED lets several replicas run the relay at the same time:
// each one grabs different rows instead of waiting on each other's locks.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	var count int
	err := postgres.InTx(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id, event_id::text, topic, msg_key, event_type, payload, headers
			   FROM outbox
			  WHERE published_at IS NULL
			  ORDER BY id
			  LIMIT $1
			  FOR UPDATE SKIP LOCKED`, r.cfg.BatchSize)
		if err != nil {
			return err
		}
		var ids []int64
		var msgs []kafka.Message
		for rows.Next() {
			var (
				id      int64
				m       Message
				headers []byte
			)
			if err := rows.Scan(&id, &m.EventID, &m.Topic, &m.Key, &m.EventType, &m.Payload, &headers); err != nil {
				rows.Close()
				return err
			}
			h := map[string]string{}
			_ = json.Unmarshal(headers, &h)
			ids = append(ids, id)
			msgs = append(msgs, kafka.Message{Topic: m.Topic, Key: m.Key, Value: m.Payload, Headers: h})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		if err := r.pub.Publish(ctx, msgs...); err != nil {
			return err // rollback: rows stay unpublished and are retried next tick
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return err
		}
		count = len(msgs)
		return nil
	})
	if err == nil && count > 0 {
		metricPublished.Add(float64(count))
		r.log.Debug("published outbox batch", "count", count)
	}
	return count, err
}

func (r *Relay) cleanup(ctx context.Context) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - $1::interval`,
		fmt.Sprintf("%d seconds", int(r.cfg.Retention.Seconds())))
	if err != nil {
		r.log.Warn("outbox cleanup failed", "err", err)
		return
	}
	if tag.RowsAffected() > 0 {
		r.log.Info("outbox cleanup", "deleted", tag.RowsAffected())
	}
}
