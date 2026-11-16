// payment-service: consumes orders.v1, charges/refunds via the gateway and
// publishes results to payments.v1 through its own outbox.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/umarbek-x/gRPC-Kafka_test/internal/payment"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/config"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/lifecycle"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/observability"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/postgres"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

type appConfig struct {
	LogLevel           string
	OpsAddr            string
	PostgresDSN        string
	PostgresMaxConn    int
	KafkaBrokers       []string
	ConsumerWorkers    int
	OutboxPoll         time.Duration
	GatewayLatency     time.Duration
	GatewayFailureRate float64
	DeclineAboveMinor  int
	ShutdownTimeout    time.Duration
}

func loadConfig() (appConfig, error) {
	var l config.Loader
	c := appConfig{
		LogLevel:           l.String("LOG_LEVEL", "info"),
		OpsAddr:            l.String("OPS_ADDR", ":18082"),
		PostgresDSN:        l.String("POSTGRES_DSN", "postgres://orderflow:orderflow@localhost:55432/payments?sslmode=disable"),
		PostgresMaxConn:    l.Int("POSTGRES_MAX_CONNS", 10),
		KafkaBrokers:       l.Strings("KAFKA_BROKERS", []string{"localhost:19092"}),
		ConsumerWorkers:    l.Int("CONSUMER_WORKERS", 8),
		OutboxPoll:         l.Duration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond),
		GatewayLatency:     l.Duration("GATEWAY_LATENCY", 50*time.Millisecond),
		GatewayFailureRate: l.Float("GATEWAY_FAILURE_RATE", 0.1), // 10% transient failures to exercise retries
		DeclineAboveMinor:  l.Int("GATEWAY_DECLINE_ABOVE_MINOR", 100_000),
		ShutdownTimeout:    l.Duration("SHUTDOWN_TIMEOUT", 20*time.Second),
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
	log := observability.NewLogger(payment.ServiceName, cfg.LogLevel)
	group := lifecycle.NewGroup(context.Background(), log)
	ctx := group.Context()

	pool, err := postgres.Connect(ctx, cfg.PostgresDSN, int32(cfg.PostgresMaxConn), log)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool, payment.Migrations, payment.MigrationsDir, log); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	producer := kafka.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	gateway := payment.NewFakeGateway(cfg.GatewayLatency, cfg.GatewayFailureRate, int64(cfg.DeclineAboveMinor))
	handler := payment.NewHandler(payment.NewPostgresStore(pool), gateway, log)

	consumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		GroupID:  payment.ServiceName,
		Topics:   []string{events.TopicOrders},
		Workers:  cfg.ConsumerWorkers,
		Retry:    retry.Policy{MaxAttempts: 6, Initial: 200 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2, Jitter: true},
		DLQTopic: payment.ServiceName + ".dlq",
	}, handler.Handle, producer, log)

	relay := outbox.NewRelay(pool, producer, outbox.RelayConfig{PollInterval: cfg.OutboxPoll}, log)

	ops := observability.NewOpsServer(cfg.OpsAddr, log)
	ops.AddCheck(observability.Check{Name: "postgres", Fn: pool.Ping})

	group.Go("ops-http", ops.Run)
	group.Go("outbox-relay", relay.Run)
	group.Go("kafka-consumer", consumer.Run)
	ops.SetReady(true)
	log.Info("payment-service started")

	group.WaitForShutdown()
	log.Info("shutting down")
	ops.SetReady(false)
	err = group.Wait(cfg.ShutdownTimeout)
	log.Info("shutdown complete")
	return err
}
