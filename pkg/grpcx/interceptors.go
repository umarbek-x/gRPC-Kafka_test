// Package grpcx contains gRPC server interceptors (middleware).
//
// An interceptor wraps every RPC, like HTTP middleware. Cross-cutting concerns
// (logging, metrics, panic recovery, request IDs, auth) live here so handlers
// contain only business logic. They are chained in order:
//
//	request → RequestID → Logging → Metrics → Recovery → handler
package grpcx

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/observability"
)

// RequestIDHeader is the gRPC metadata key carrying the correlation ID.
const RequestIDHeader = "x-request-id"

var (
	metricRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_server_requests_total", Help: "gRPC requests by method and status code.",
	}, []string{"method", "code"})
	metricLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "grpc_server_request_seconds", Help: "gRPC request latency.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"method"})
)

func withRequestID(ctx context.Context) context.Context {
	var id string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(RequestIDHeader); len(v) > 0 {
			id = v[0]
		}
	}
	if id != "" {
		ctx = observability.WithCorrelationID(ctx, id)
	} else {
		ctx, id = observability.EnsureCorrelationID(ctx)
	}
	// Echo it back so the client can quote it in a bug report.
	_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, id))
	return ctx
}

// UnaryRequestID reads x-request-id from metadata (or generates one) and
// stores it in the context as the correlation ID.
func UnaryRequestID() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(withRequestID(ctx), req)
	}
}

// UnaryLogging logs one line per RPC and records metrics.
func UnaryLogging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		observe(ctx, log, info.FullMethod, start, err)
		return resp, err
	}
}

// UnaryRecovery converts a panic in a handler into codes.Internal instead of
// crashing the whole process (one bad request must not kill all others).
func UnaryRecovery(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "panic in grpc handler", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// wrappedStream overrides Context() so stream handlers see our enriched ctx.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// StreamRequestID is UnaryRequestID for streaming RPCs.
func StreamRequestID() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: withRequestID(ss.Context())})
	}
}

// StreamLogging is UnaryLogging for streaming RPCs.
func StreamLogging(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		observe(ss.Context(), log, info.FullMethod, start, err)
		return err
	}
}

// StreamRecovery is UnaryRecovery for streaming RPCs.
func StreamRecovery(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ss.Context(), "panic in grpc stream handler", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}

func observe(ctx context.Context, log *slog.Logger, method string, start time.Time, err error) {
	code := status.Code(err)
	dur := time.Since(start)
	metricRequests.WithLabelValues(method, code.String()).Inc()
	metricLatency.WithLabelValues(method).Observe(dur.Seconds())

	level := slog.LevelInfo
	switch code {
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unavailable:
		level = slog.LevelError // our fault: page someone
	case codes.OK, codes.Canceled:
	default:
		level = slog.LevelWarn // client's fault: worth watching
	}
	attrs := []any{"method", method, "code", code.String(), "duration_ms", float64(dur.Microseconds()) / 1000}
	if err != nil {
		attrs = append(attrs, "err", err.Error())
	}
	log.Log(ctx, level, "grpc request", attrs...)
}
