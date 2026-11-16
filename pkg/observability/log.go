// Package observability contains logging, correlation IDs, metrics and the
// operational HTTP server (health checks + Prometheus metrics).
//
// The three pillars of observability are logs, metrics and traces. This
// project uses structured JSON logs (log/slog), Prometheus metrics, and a
// correlation ID propagated through gRPC metadata and Kafka headers, which is
// a lightweight stand-in for distributed tracing (OpenTelemetry).
package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
)

type ctxKey struct{}

// WithCorrelationID stores the correlation ID in ctx.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the correlation ID from ctx, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// EnsureCorrelationID returns ctx with a correlation ID, generating one if absent.
func EnsureCorrelationID(ctx context.Context) (context.Context, string) {
	if id := CorrelationID(ctx); id != "" {
		return ctx, id
	}
	id := uuid.NewString()
	return WithCorrelationID(ctx, id), id
}

// NewLogger creates a JSON logger. Every record logged with a *Context method
// (InfoContext, ErrorContext...) automatically gets "correlation_id".
func NewLogger(service, level string) *slog.Logger {
	return newLogger(os.Stdout, service, level)
}

func newLogger(w io.Writer, service, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(&ctxHandler{Handler: h}).With("service", service)
}

// ctxHandler decorates a slog.Handler: it reads values from the context and
// adds them as attributes. This is the decorator pattern.
type ctxHandler struct{ slog.Handler }

func (h *ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *ctxHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return &ctxHandler{h.Handler.WithAttrs(a)}
}

func (h *ctxHandler) WithGroup(name string) slog.Handler {
	return &ctxHandler{h.Handler.WithGroup(name)}
}
