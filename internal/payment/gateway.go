package payment

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/ds/lru"
)

// ChargeRequest asks the gateway to capture money.
type ChargeRequest struct {
	// IdempotencyKey makes Charge safe to retry: the gateway returns the
	// first result for a repeated key instead of charging twice. Real
	// providers (Stripe, Adyen) offer exactly this.
	IdempotencyKey string
	AmountMinor    int64
	Currency       string
}

// ChargeResult is a BUSINESS outcome. A declined card is not an error: the
// call worked and the answer is "no". Errors are reserved for "we don't
// know what happened" (timeout, 5xx), which is what gets retried.
type ChargeResult struct {
	Approved      bool
	DeclineReason string
}

// Gateway abstracts the external payment provider.
type Gateway interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
	Refund(ctx context.Context, paymentID string) error
}

// ErrGatewayUnavailable simulates a transient provider failure.
var ErrGatewayUnavailable = errors.New("payment gateway unavailable")

// FakeGateway simulates a provider: latency, random transient failures and a
// deterministic decline rule (amount above the limit => insufficient funds).
type FakeGateway struct {
	latency      time.Duration
	failureRate  float64 // probability of a transient error per call
	declineAbove int64
	results      *lru.Cache[string, ChargeResult] // idempotency-key -> first result
}

func NewFakeGateway(latency time.Duration, failureRate float64, declineAbove int64) *FakeGateway {
	return &FakeGateway{
		latency: latency, failureRate: failureRate, declineAbove: declineAbove,
		results: lru.New[string, ChargeResult](100_000, 24*time.Hour),
	}
}

func (g *FakeGateway) simulateNetwork(ctx context.Context) error {
	if g.latency > 0 {
		// Sleep, but wake up immediately if the caller gives up (context).
		t := time.NewTimer(g.latency/2 + rand.N(g.latency))
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	if g.failureRate > 0 && rand.Float64() < g.failureRate {
		return ErrGatewayUnavailable
	}
	return nil
}

func (g *FakeGateway) Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error) {
	if err := g.simulateNetwork(ctx); err != nil {
		return ChargeResult{}, err
	}
	if res, ok := g.results.Get(req.IdempotencyKey); ok {
		return res, nil // replay: same answer, no second charge
	}
	res := ChargeResult{Approved: true}
	if req.AmountMinor > g.declineAbove {
		res = ChargeResult{Approved: false, DeclineReason: "insufficient_funds"}
	}
	g.results.Put(req.IdempotencyKey, res)
	return res, nil
}

func (g *FakeGateway) Refund(ctx context.Context, _ string) error {
	return g.simulateNetwork(ctx)
}
