package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/workerpool"
)

// Handler processes one message. Return:
//   - nil                    -> success, offset will be committed
//   - retry.Permanent(err)   -> no retry, message goes straight to the DLQ
//   - any other error        -> retried with backoff, then DLQ
//
// Handlers MUST be idempotent: Kafka gives at-least-once delivery, so the same
// message can arrive twice (after a crash or rebalance).
type Handler func(ctx context.Context, msg Message) error

// DLQ headers describing why a message was dead-lettered.
const (
	HeaderDLQOriginalTopic     = "x-original-topic"
	HeaderDLQOriginalPartition = "x-original-partition"
	HeaderDLQOriginalOffset    = "x-original-offset"
	HeaderDLQError             = "x-error"
	HeaderDLQAttempts          = "x-attempts"
	HeaderDLQConsumerGroup     = "x-consumer-group"
)

type ConsumerConfig struct {
	Brokers []string
	GroupID string
	Topics  []string

	Workers   int // parallelism (number of shards in the keyed pool)
	QueueSize int // buffered tasks per worker (backpressure threshold)

	Retry          retry.Policy
	DLQTopic       string // "" disables the DLQ (message is skipped after retries)
	CommitInterval time.Duration
	HandlerTimeout time.Duration
}

var (
	metricProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_messages_total",
		Help: "Messages processed by result (ok, retried_ok, dlq, skipped).",
	}, []string{"group", "topic", "result"})
	metricDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_consumer_handle_seconds",
		Help:    "Time to handle one message, including retries.",
		Buckets: prometheus.DefBuckets,
	}, []string{"group", "topic"})
	metricFetchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_consumer_fetch_errors_total",
		Help: "Failed fetches (broker unreachable, etc.). Alert if it keeps growing.",
	}, []string{"group"})
	metricInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_in_flight",
		Help: "Fetched messages not yet committed.",
	}, []string{"group"})
)

// Consumer reads from a consumer group and processes messages concurrently.
//
//	                 ┌──────── worker 0 ─┐
//	Kafka ─► fetch ─►┼─ hash(key) ─ worker 1 ─┼─► handler ─► tracker.Done
//	   loop          └──────── worker N ─┘                    │
//	                                                         ▼
//	                                   commit loop (every CommitInterval)
type Consumer struct {
	cfg     ConsumerConfig
	reader  *kafkago.Reader
	dlq     *Producer
	handler Handler
	tracker *offsetTracker
	log     *slog.Logger
}

func NewConsumer(cfg ConsumerConfig, handler Handler, dlq *Producer, log *slog.Logger) *Consumer {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 16
	}
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = time.Second
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = 30 * time.Second
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:           cfg.Brokers,
		GroupID:           cfg.GroupID,
		GroupTopics:       cfg.Topics,
		MinBytes:          1,
		MaxBytes:          10 << 20, // 10 MiB
		MaxWait:           500 * time.Millisecond,
		StartOffset:       kafkago.FirstOffset, // new group starts from the beginning
		CommitInterval:    0,                   // we commit explicitly (see commitLoop)
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
	})
	return &Consumer{
		cfg: cfg, reader: r, dlq: dlq, handler: handler,
		tracker: newOffsetTracker(),
		log:     log.With("component", "kafka-consumer", "group", cfg.GroupID),
	}
}

// Run blocks until ctx is cancelled. Shutdown sequence:
//  1. stop fetching new messages
//  2. let workers finish what they already have (drain)
//  3. commit the final offsets
//  4. close the reader (leave the group -> fast rebalance)
func (c *Consumer) Run(ctx context.Context) error {
	pool := workerpool.NewKeyed(c.cfg.Workers, c.cfg.QueueSize, func(r any) {
		c.log.Error("handler panicked", "panic", r)
	})

	// Handlers use a context that is NOT cancelled on shutdown, so in-flight
	// work can finish cleanly instead of failing halfway through.
	workCtx := context.WithoutCancel(ctx)

	commitDone := make(chan struct{})
	commitCtx, stopCommit := context.WithCancel(workCtx)
	go func() {
		defer close(commitDone)
		c.commitLoop(commitCtx)
	}()

	c.log.Info("consumer started", "topics", c.cfg.Topics, "workers", c.cfg.Workers)
	// A broker outage must not crash the service (the gRPC API and the rest
	// keep working without Kafka). Fetch errors are retried forever with a
	// capped backoff; only cancellation ends the loop.
	fetchBackoff := retry.Policy{Initial: 500 * time.Millisecond, Max: 10 * time.Second, Multiplier: 2, Jitter: true}
	fetchFailures := 0
	for {
		km, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				break
			}
			fetchFailures++
			wait := fetchBackoff.Backoff(fetchFailures)
			c.log.Warn("fetch failed, will retry", "failures", fetchFailures, "wait", wait.String(), "err", err)
			metricFetchErrors.WithLabelValues(c.cfg.GroupID).Inc()
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
			if ctx.Err() != nil {
				break
			}
			continue
		}
		fetchFailures = 0
		msg := Message{
			Topic: km.Topic, Key: string(km.Key), Value: km.Value,
			Headers: fromKafkaHeaders(km.Headers), Partition: km.Partition,
			Offset: km.Offset, Time: km.Time,
		}
		tp := TopicPartition{Topic: km.Topic, Partition: km.Partition}
		c.tracker.Track(tp, km.Offset)

		// Submit blocks when the worker's queue is full -> we stop fetching
		// -> backpressure all the way to Kafka.
		if err := pool.Submit(ctx, msg.Key, func() {
			c.process(workCtx, msg)
			c.tracker.Done(tp, msg.Offset)
		}); err != nil {
			break // ctx cancelled while waiting for a free slot
		}
		metricInFlight.WithLabelValues(c.cfg.GroupID).Set(float64(c.tracker.InFlight()))
	}

	c.log.Info("consumer stopping: draining workers")
	pool.Close()
	stopCommit()
	<-commitDone
	c.commit(workCtx) // final commit after drain
	if err := c.reader.Close(); err != nil {
		c.log.Warn("close reader", "err", err)
	}
	c.log.Info("consumer stopped")
	return nil
}

func (c *Consumer) process(ctx context.Context, msg Message) {
	start := time.Now()
	defer func() {
		metricDuration.WithLabelValues(c.cfg.GroupID, msg.Topic).Observe(time.Since(start).Seconds())
	}()
	log := c.log.With("topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", msg.Key)

	attempts, err := retry.Do(ctx, c.cfg.Retry, func(ctx context.Context) error {
		hctx, cancel := context.WithTimeout(ctx, c.cfg.HandlerTimeout)
		defer cancel()
		return c.handler(hctx, msg)
	}, func(attempt int, err error, wait time.Duration) {
		log.Warn("handler failed, will retry", "attempt", attempt, "wait", wait.String(), "err", err)
	})

	switch {
	case err == nil && attempts == 1:
		metricProcessed.WithLabelValues(c.cfg.GroupID, msg.Topic, "ok").Inc()
	case err == nil:
		metricProcessed.WithLabelValues(c.cfg.GroupID, msg.Topic, "retried_ok").Inc()
	default:
		log.Error("message failed permanently", "attempts", attempts, "permanent", retry.IsPermanent(err), "err", err)
		c.deadLetter(ctx, msg, attempts, err, log)
	}
}

// deadLetter parks a message we cannot process in a separate topic so the
// partition is not blocked forever ("poison pill"). An operator can inspect,
// fix and replay it later.
func (c *Consumer) deadLetter(ctx context.Context, msg Message, attempts int, cause error, log *slog.Logger) {
	if c.dlq == nil || c.cfg.DLQTopic == "" {
		metricProcessed.WithLabelValues(c.cfg.GroupID, msg.Topic, "skipped").Inc()
		return
	}
	headers := make(map[string]string, len(msg.Headers)+6)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	headers[HeaderDLQOriginalTopic] = msg.Topic
	headers[HeaderDLQOriginalPartition] = strconv.Itoa(msg.Partition)
	headers[HeaderDLQOriginalOffset] = strconv.FormatInt(msg.Offset, 10)
	headers[HeaderDLQError] = cause.Error()
	headers[HeaderDLQAttempts] = strconv.Itoa(attempts)
	headers[HeaderDLQConsumerGroup] = c.cfg.GroupID

	// Publishing to the DLQ must not be lost either: retry until it works.
	_, err := retry.Do(ctx, retry.Policy{MaxAttempts: 10, Initial: 200 * time.Millisecond, Max: 10 * time.Second, Multiplier: 2, Jitter: true},
		func(ctx context.Context) error {
			return c.dlq.Publish(ctx, Message{Topic: c.cfg.DLQTopic, Key: msg.Key, Value: msg.Value, Headers: headers})
		}, nil)
	if err != nil {
		log.Error("DLQ publish failed; message dropped", "err", err)
		metricProcessed.WithLabelValues(c.cfg.GroupID, msg.Topic, "dropped").Inc()
		return
	}
	metricProcessed.WithLabelValues(c.cfg.GroupID, msg.Topic, "dlq").Inc()
}

func (c *Consumer) commitLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.CommitInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.commit(ctx)
		}
	}
}

func (c *Consumer) commit(ctx context.Context) {
	positions := c.tracker.Committable()
	if len(positions) == 0 {
		return
	}
	msgs := make([]kafkago.Message, len(positions))
	for i, p := range positions {
		// kafka-go commits msg.Offset+1 ("next offset to read").
		msgs[i] = kafkago.Message{Topic: p.Topic, Partition: p.Partition, Offset: p.Offset}
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.reader.CommitMessages(cctx, msgs...); err != nil {
		// Not fatal: worst case those messages are redelivered (at-least-once)
		// and idempotent handlers ignore the duplicates.
		c.log.Warn("offset commit failed", "err", err)
		return
	}
	c.log.Debug("committed offsets", "positions", positions)
}
