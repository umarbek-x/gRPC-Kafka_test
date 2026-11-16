// Package retry implements retries with exponential backoff and jitter.
//
// Errors fall into two classes:
//   - transient (timeouts, connection refused, deadlocks): worth retrying.
//   - permanent (validation failed, unknown event type): retrying can never
//     succeed. Wrap them with Permanent(err) so Do stops immediately.
//
// Backoff grows exponentially: initial, initial*m, initial*m², ... capped at
// Max. "Full jitter" picks a random delay in [0, backoff] so that many clients
// failing at the same moment don't all retry at the same moment
// (the "thundering herd" problem).
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Policy configures retries.
type Policy struct {
	MaxAttempts int           // total attempts including the first; <=0 means 1
	Initial     time.Duration // first backoff
	Max         time.Duration // backoff cap
	Multiplier  float64       // growth factor, e.g. 2
	Jitter      bool          // use full jitter
}

// DefaultPolicy is a sensible default for calls to databases/brokers.
var DefaultPolicy = Policy{MaxAttempts: 5, Initial: 100 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2, Jitter: true}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent marks err as non-retryable. Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// Backoff returns the delay before attempt n+1 (n starts at 1).
func (p Policy) Backoff(n int) time.Duration {
	d := float64(p.Initial)
	for i := 1; i < n; i++ {
		d *= p.Multiplier
		if p.Max > 0 && d > float64(p.Max) {
			d = float64(p.Max)
			break
		}
	}
	if p.Max > 0 && d > float64(p.Max) {
		d = float64(p.Max)
	}
	if p.Jitter && d > 0 {
		d = rand.Float64() * d
	}
	return time.Duration(d)
}

// Do calls fn until it succeeds, returns a permanent error, attempts are
// exhausted, or ctx is cancelled. onRetry (optional) is called before each
// sleep, which is a good place for logging and metrics.
// It returns the number of attempts made and the last error.
func Do(ctx context.Context, p Policy, fn func(ctx context.Context) error, onRetry func(attempt int, err error, wait time.Duration)) (int, error) {
	max := p.MaxAttempts
	if max <= 0 {
		max = 1
	}
	var err error
	for attempt := 1; ; attempt++ {
		if err = fn(ctx); err == nil {
			return attempt, nil
		}
		if IsPermanent(err) || attempt >= max {
			return attempt, err
		}
		wait := p.Backoff(attempt)
		if onRetry != nil {
			onRetry(attempt, err, wait)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempt, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}
