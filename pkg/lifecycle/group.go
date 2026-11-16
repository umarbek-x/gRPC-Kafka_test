// Package lifecycle runs a service's long-lived goroutines as one unit.
//
// A service is several loops running at once (gRPC server, Kafka consumer,
// outbox relay, HTTP server). The rules we want:
//
//   - if ANY of them fails, stop ALL of them (a half-alive service is worse
//     than a restarted one);
//   - on SIGINT/SIGTERM stop all of them gracefully;
//   - wait for them to finish, but not forever.
//
// (golang.org/x/sync/errgroup offers the same idea; this version adds names,
// logging and a bounded wait, and is small enough to read in one sitting.)
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Group struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	stop   context.CancelFunc
	wg     sync.WaitGroup
	log    *slog.Logger
}

// NewGroup returns a group whose context is cancelled on SIGINT (Ctrl+C) or
// SIGTERM (`docker stop`, Kubernetes pod termination).
func NewGroup(parent context.Context, log *slog.Logger) *Group {
	sigCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancelCause(sigCtx)
	return &Group{ctx: ctx, cancel: cancel, stop: stop, log: log}
}

// Context is cancelled when the group is shutting down.
func (g *Group) Context() context.Context { return g.ctx }

// Go runs fn in a goroutine. If fn returns a real error the whole group is
// cancelled with that error as the cause.
func (g *Group) Go(name string, fn func(ctx context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn(g.ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			g.log.Error("component failed", "component", name, "err", err)
			g.cancel(fmt.Errorf("%s: %w", name, err))
			return
		}
		g.log.Debug("component stopped", "component", name)
	}()
}

// WaitForShutdown blocks until a signal arrives or a component fails.
func (g *Group) WaitForShutdown() {
	<-g.ctx.Done()
	g.stop() // restore default signal handling: a second Ctrl+C kills immediately
}

// Wait waits up to timeout for all goroutines and returns the failure that
// triggered the shutdown, or nil for a normal signal-initiated stop.
func (g *Group) Wait(timeout time.Duration) error {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		g.log.Warn("components did not stop in time", "timeout", timeout)
	}
	if cause := context.Cause(g.ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}
