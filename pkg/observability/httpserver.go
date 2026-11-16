package observability

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check is a named readiness check (e.g. "postgres" -> pool.Ping).
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

// OpsServer serves operational endpoints on a separate port from business
// traffic so that they are never exposed publicly:
//
//	GET /healthz  liveness: "the process is alive" (Kubernetes restarts it if not)
//	GET /readyz   readiness: "dependencies are reachable" (removed from load balancer if not)
//	GET /metrics  Prometheus scrape endpoint
type OpsServer struct {
	srv    *http.Server
	mux    *http.ServeMux
	log    *slog.Logger
	mu     sync.RWMutex
	checks []Check
	ready  bool
}

func NewOpsServer(addr string, log *slog.Logger) *OpsServer {
	o := &OpsServer{mux: http.NewServeMux(), log: log}
	o.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	o.mux.HandleFunc("GET /readyz", o.readyz)
	o.mux.Handle("GET /metrics", promhttp.Handler())
	o.srv = &http.Server{Addr: addr, Handler: o.mux, ReadHeaderTimeout: 5 * time.Second}
	return o
}

// Handle registers an extra endpoint (e.g. a debug page).
func (o *OpsServer) Handle(pattern string, h http.Handler) { o.mux.Handle(pattern, h) }

// AddCheck registers a readiness check.
func (o *OpsServer) AddCheck(c Check) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.checks = append(o.checks, c)
}

// SetReady flips readiness. Set it false at the start of shutdown so the load
// balancer stops sending traffic before we stop serving.
func (o *OpsServer) SetReady(v bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ready = v
}

func (o *OpsServer) readyz(w http.ResponseWriter, r *http.Request) {
	o.mu.RLock()
	ready, checks := o.ready, append([]Check(nil), o.checks...)
	o.mu.RUnlock()

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	result := map[string]string{}
	ok := ready
	if !ready {
		result["server"] = "not ready"
	}
	for _, c := range checks {
		if err := c.Fn(ctx); err != nil {
			result[c.Name] = err.Error()
			ok = false
		} else {
			result[c.Name] = "ok"
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(result)
}

// Run serves until ctx is cancelled, then shuts down gracefully.
func (o *OpsServer) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		o.log.Info("ops http server listening", "addr", o.srv.Addr)
		if err := o.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return o.srv.Shutdown(shutdownCtx)
}
