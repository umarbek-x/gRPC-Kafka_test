// order-service: gRPC API for orders + outbox relay + payment event consumer.
//
// main() is the COMPOSITION ROOT: the only place that knows every concrete
// type. It builds the dependency graph by hand (no DI framework needed),
// starts the long-running goroutines and coordinates graceful shutdown.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	orderv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/order/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/eventhandler"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/grpcapi"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/repository"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/service"
	"github.com/umarbek-x/gRPC-Kafka_test/internal/order/watch"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/config"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/events"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/grpcx"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/lifecycle"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/observability"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/postgres"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/retry"
)

type appConfig struct {
	LogLevel        string
	GRPCAddr        string
	OpsAddr         string
	PostgresDSN     string
	PostgresMaxConn int
	KafkaBrokers    []string
	ConsumerWorkers int
	OutboxPoll      time.Duration
	CacheSize       int
	CacheTTL        time.Duration
	ShutdownTimeout time.Duration
}

func loadConfig() (appConfig, error) {
	var l config.Loader
	c := appConfig{
		LogLevel:        l.String("LOG_LEVEL", "info"),
		GRPCAddr:        l.String("GRPC_ADDR", ":50061"),
		OpsAddr:         l.String("OPS_ADDR", ":18081"),
		PostgresDSN:     l.String("POSTGRES_DSN", "postgres://orderflow:orderflow@localhost:55432/orders?sslmode=disable"),
		PostgresMaxConn: l.Int("POSTGRES_MAX_CONNS", 10),
		KafkaBrokers:    l.Strings("KAFKA_BROKERS", []string{"localhost:19092"}),
		ConsumerWorkers: l.Int("CONSUMER_WORKERS", 8),
		OutboxPoll:      l.Duration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond),
		CacheSize:       l.Int("ORDER_CACHE_SIZE", 10_000),
		CacheTTL:        l.Duration("ORDER_CACHE_TTL", 30*time.Second),
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

// run holds the real logic so that deferred cleanups execute before exit
// (os.Exit inside main would skip them).
func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	log := observability.NewLogger(service.ServiceName, cfg.LogLevel)
	group := lifecycle.NewGroup(context.Background(), log)
	ctx := group.Context()

	// --- infrastructure ---------------------------------------------------
	pool, err := postgres.Connect(ctx, cfg.PostgresDSN, int32(cfg.PostgresMaxConn), log)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool, repository.Migrations, repository.MigrationsDir, log); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	producer := kafka.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	// --- application ------------------------------------------------------
	svc := service.New(repository.New(pool), watch.NewBroker(), cfg.CacheSize, cfg.CacheTTL, log)

	relay := outbox.NewRelay(pool, producer, outbox.RelayConfig{PollInterval: cfg.OutboxPoll}, log)

	consumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		GroupID:  service.ServiceName,
		Topics:   []string{events.TopicPayments},
		Workers:  cfg.ConsumerWorkers,
		Retry:    retry.Policy{MaxAttempts: 5, Initial: 200 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2, Jitter: true},
		DLQTopic: service.ServiceName + ".dlq",
	}, eventhandler.New(svc, log).Handle, producer, log)

	// --- gRPC server ------------------------------------------------------
	grpcServer := grpc.NewServer(
		// Interceptors run in this order for every call.
		grpc.ChainUnaryInterceptor(grpcx.UnaryRequestID(), grpcx.UnaryLogging(log), grpcx.UnaryRecovery(log)),
		grpc.ChainStreamInterceptor(grpcx.StreamRequestID(), grpcx.StreamLogging(log), grpcx.StreamRecovery(log)),
		// Keepalive detects dead connections and recycles long-lived ones so
		// load balancers can redistribute them across replicas.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 30 * time.Second, Timeout: 10 * time.Second,
			MaxConnectionAge: 30 * time.Minute, MaxConnectionAgeGrace: time.Minute,
		}),
		grpc.MaxRecvMsgSize(1<<20), // 1 MiB: reject oversized requests early
	)
	orderv1.RegisterOrderServiceServer(grpcServer, grpcapi.NewServer(svc, log))

	// Standard gRPC health protocol (Kubernetes gRPC probes, load balancers).
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	// Reflection lets grpcurl/Postman discover the API without .proto files.
	// Many companies enable it only outside production.
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCAddr, err)
	}

	ops := observability.NewOpsServer(cfg.OpsAddr, log)
	ops.AddCheck(observability.Check{Name: "postgres", Fn: pool.Ping})

	// --- run ----------------------------------------------------------------
	group.Go("ops-http", ops.Run)
	group.Go("outbox-relay", relay.Run)
	group.Go("kafka-consumer", consumer.Run)
	group.Go("grpc", func(context.Context) error {
		log.Info("grpc server listening", "addr", cfg.GRPCAddr)
		return grpcServer.Serve(lis) // returns nil after GracefulStop
	})

	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	ops.SetReady(true)
	log.Info("order-service started")

	group.WaitForShutdown()

	// --- graceful shutdown ------------------------------------------------
	// 1. report "not ready" so load balancers stop routing to us
	// 2. stop accepting RPCs and let in-flight ones finish (bounded)
	// 3. consumer and relay finish their current work (they watch ctx)
	log.Info("shutting down")
	ops.SetReady(false)
	healthSrv.Shutdown()

	stopped := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("graceful stop timed out; forcing")
		grpcServer.Stop()
	}

	err = group.Wait(cfg.ShutdownTimeout)
	log.Info("shutdown complete")
	return err
}
