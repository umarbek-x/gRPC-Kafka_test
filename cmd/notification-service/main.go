// notification-service: consumes orders.v1 + payments.v1 and "sends"
// notifications in priority order under a rate limit. Stateless.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/umarbek-x/gRPC-Kafka_test/internal/notification"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/config"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/lifecycle"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/observability"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/ratelimit"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

type appConfig struct {
	LogLevel        string
	OpsAddr         string
	KafkaBrokers    []string
	ConsumerWorkers int
	Dispatchers     int
	RatePerSecond   float64
	Burst           int
	SendLatency     time.Duration
	ShutdownTimeout time.Duration
}

func loadConfig() (appConfig, error) {
	var l config.Loader
	c := appConfig{
		LogLevel:        l.String("LOG_LEVEL", "info"),
		OpsAddr:         l.String("OPS_ADDR", ":18083"),
		KafkaBrokers:    l.Strings("KAFKA_BROKERS", []string{"localhost:19092"}),
		ConsumerWorkers: l.Int("CONSUMER_WORKERS", 16),
		Dispatchers:     l.Int("DISPATCHERS", 4),
		RatePerSecond:   l.Float("SEND_RATE_PER_SECOND", 50),
		Burst:           l.Int("SEND_BURST", 10),
		SendLatency:     l.Duration("SEND_LATENCY", 20*time.Millisecond),
		ShutdownTimeout: l.Duration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	return c, l.Err()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	log := observability.NewLogger(notification.ServiceName, cfg.LogLevel)
	group := lifecycle.NewGroup(context.Background(), log)

	producer := kafka.NewProducer(cfg.KafkaBrokers) // only used for the DLQ
	defer producer.Close()

	scheduler := notification.NewScheduler(
		notification.LogSender{Log: log, Latency: cfg.SendLatency},
		ratelimit.New(cfg.RatePerSecond, cfg.Burst),
	)
	handler := notification.NewHandler(scheduler, 100_000, 200, log)

	consumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		GroupID:  notification.ServiceName,
		Topics:   []string{events.TopicOrders, events.TopicPayments},
		Workers:  cfg.ConsumerWorkers,
		Retry:    retry.Policy{MaxAttempts: 4, Initial: 500 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2, Jitter: true},
		DLQTopic: notification.ServiceName + ".dlq",
	}, handler.Handle, producer, log)

	ops := observability.NewOpsServer(cfg.OpsAddr, log)
	// Debug endpoint backed by the ring buffer: the last 200 notifications.
	ops.Handle("GET /debug/notifications", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(handler.Recent())
	}))

	// SHUTDOWN ORDER MATTERS. The consumer's workers call scheduler.Submit and
	// wait for the result. If the scheduler stopped at the same moment as the
	// consumer, the draining workers would be waiting on a dead scheduler.
	// So the scheduler gets its OWN context and is stopped only after the
	// consumer has fully drained: stop producers of work before consumers of work.
	schedCtx, stopScheduler := context.WithCancel(context.Background())
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		_ = scheduler.Run(schedCtx, cfg.Dispatchers)
	}()

	group.Go("ops-http", ops.Run)
	group.Go("kafka-consumer", consumer.Run)
	ops.SetReady(true)
	log.Info("notification-service started", "rate_per_second", cfg.RatePerSecond)

	group.WaitForShutdown()
	log.Info("shutting down")
	ops.SetReady(false)
	err = group.Wait(cfg.ShutdownTimeout) // consumer drains first...
	stopScheduler()                       // ...then the scheduler stops
	<-schedDone
	log.Info("shutdown complete")
	return err
}
