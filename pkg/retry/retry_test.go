package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

var fast = Policy{MaxAttempts: 4, Initial: time.Millisecond, Max: 4 * time.Millisecond, Multiplier: 2}

func TestSucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	attempts, err := Do(context.Background(), fast, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}, nil)
	if err != nil || attempts != 3 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}

func TestStopsOnPermanentError(t *testing.T) {
	base := errors.New("bad input")
	attempts, err := Do(context.Background(), fast, func(context.Context) error {
		return Permanent(base)
	}, nil)
	if attempts != 1 || !errors.Is(err, base) || !IsPermanent(err) {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var retries []int
	attempts, err := Do(context.Background(), fast, func(context.Context) error {
		return errors.New("always fails")
	}, func(a int, _ error, _ time.Duration) { retries = append(retries, a) })
	if attempts != 4 || err == nil {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	if len(retries) != 3 {
		t.Fatalf("onRetry called %d times, want 3", len(retries))
	}
}

func TestContextCancellationStopsRetrying(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := Policy{MaxAttempts: 100, Initial: time.Hour, Multiplier: 1}
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	_, err := Do(ctx, p, func(context.Context) error { return errors.New("x") }, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	p := Policy{Initial: 10 * time.Millisecond, Max: 50 * time.Millisecond, Multiplier: 2}
	want := []time.Duration{10, 20, 40, 50, 50}
	for i, w := range want {
		if got := p.Backoff(i + 1); got != w*time.Millisecond {
			t.Fatalf("backoff(%d)=%v want %v", i+1, got, w*time.Millisecond)
		}
	}
}

func TestJitterStaysInRange(t *testing.T) {
	p := Policy{Initial: 10 * time.Millisecond, Max: time.Second, Multiplier: 2, Jitter: true}
	for i := 0; i < 100; i++ {
		if d := p.Backoff(3); d < 0 || d > 40*time.Millisecond {
			t.Fatalf("jittered backoff %v out of range", d)
		}
	}
}
