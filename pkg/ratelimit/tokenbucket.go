// Package ratelimit implements the token-bucket algorithm.
//
// A bucket holds up to `burst` tokens and refills at `rate` tokens/second.
// Each operation takes one token; when the bucket is empty the caller waits
// until a token is refilled. This allows short bursts while enforcing an
// average rate — exactly how email/SMS providers limit API calls.
//
// Instead of a background goroutine that adds tokens on a ticker, we refill
// lazily: on each call we compute how many tokens accumulated since the last
// call (elapsed * rate). No goroutine, no ticker, O(1) per call.
//
// (In real code you would usually use golang.org/x/time/rate, which works the
// same way. This version exists so you can read how it works.)
package ratelimit

import (
	"context"
	"sync"
	"time"
)

type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

// New creates a bucket that starts full.
func New(ratePerSecond float64, burst int) *TokenBucket {
	if ratePerSecond <= 0 || burst <= 0 {
		panic("ratelimit: rate and burst must be > 0")
	}
	return &TokenBucket{rate: ratePerSecond, burst: float64(burst), tokens: float64(burst), last: time.Now(), now: time.Now}
}

// refill must be called with mu held.
func (b *TokenBucket) refill() {
	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// Allow takes a token if one is available, without waiting.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// Wait blocks until a token is available or ctx is done.
func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		b.refill()
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		// Time until one whole token is available.
		wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// loop and try again: another goroutine may have taken the token
		}
	}
}
