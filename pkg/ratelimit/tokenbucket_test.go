package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestBurstThenRefill(t *testing.T) {
	now := time.Unix(0, 0)
	b := New(10, 3) // 10 tokens/s, burst 3
	b.now = func() time.Time { return now }
	b.last = now

	for i := 0; i < 3; i++ {
		if !b.Allow() {
			t.Fatalf("burst token %d should be allowed", i)
		}
	}
	if b.Allow() {
		t.Fatal("bucket should be empty")
	}
	now = now.Add(100 * time.Millisecond) // +1 token
	if !b.Allow() {
		t.Fatal("expected a refilled token")
	}
	now = now.Add(time.Hour) // refill is capped at burst
	for i := 0; i < 3; i++ {
		b.Allow()
	}
	if b.Allow() {
		t.Fatal("tokens must be capped at burst")
	}
}

func TestWaitEnforcesRate(t *testing.T) {
	b := New(100, 1) // one token per 10ms
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 6; i++ {
		if err := b.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// first is free (burst), 5 more need ~50ms
	if el := time.Since(start); el < 40*time.Millisecond {
		t.Fatalf("too fast: %v", el)
	}
}

func TestWaitHonoursContext(t *testing.T) {
	b := New(0.001, 1)
	b.Allow()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := b.Wait(ctx); err == nil {
		t.Fatal("expected context error")
	}
}
