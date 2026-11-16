package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolRunsAllTasks(t *testing.T) {
	p := New(4, 16, nil)
	var n atomic.Int64
	for i := 0; i < 1000; i++ {
		if err := p.Submit(context.Background(), func() { n.Add(1) }); err != nil {
			t.Fatal(err)
		}
	}
	p.Close() // waits for the queue to drain
	if n.Load() != 1000 {
		t.Fatalf("ran %d tasks, want 1000", n.Load())
	}
}

func TestPoolLimitsConcurrency(t *testing.T) {
	const workers = 3
	p := New(workers, 0, nil)
	var running, maxRunning atomic.Int64
	for i := 0; i < 30; i++ {
		_ = p.Submit(context.Background(), func() {
			cur := running.Add(1)
			for {
				m := maxRunning.Load()
				if cur <= m || maxRunning.CompareAndSwap(m, cur) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			running.Add(-1)
		})
	}
	p.Close()
	if maxRunning.Load() > workers {
		t.Fatalf("max concurrency %d > %d workers", maxRunning.Load(), workers)
	}
}

func TestSubmitBlocksAndRespectsContext(t *testing.T) {
	p := New(1, 0, nil)
	defer p.Close()
	block := make(chan struct{})
	_ = p.Submit(context.Background(), func() { <-block }) // occupies the only worker

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Submit(ctx, func() {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded (backpressure), got %v", err)
	}
	close(block)
}

func TestSubmitAfterClose(t *testing.T) {
	p := New(1, 1, nil)
	p.Close()
	if err := p.Submit(context.Background(), func() {}); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v want ErrClosed", err)
	}
}

func TestPanicDoesNotKillWorker(t *testing.T) {
	var panics atomic.Int64
	p := New(1, 4, func(any) { panics.Add(1) })
	var ran atomic.Bool
	_ = p.Submit(context.Background(), func() { panic("boom") })
	_ = p.Submit(context.Background(), func() { ran.Store(true) })
	p.Close()
	if panics.Load() != 1 || !ran.Load() {
		t.Fatalf("panics=%d ran=%v", panics.Load(), ran.Load())
	}
}

// The key property of KeyedPool: per-key ordering is preserved even though
// different keys are processed in parallel.
func TestKeyedPoolPreservesPerKeyOrder(t *testing.T) {
	p := NewKeyed(8, 4, nil)
	var mu sync.Mutex
	seen := map[string][]int{}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("order-%d", i%10)
		i := i
		_ = p.Submit(context.Background(), key, func() {
			mu.Lock()
			seen[key] = append(seen[key], i)
			mu.Unlock()
		})
	}
	p.Close()

	for key, seq := range seen {
		for j := 1; j < len(seq); j++ {
			if seq[j] < seq[j-1] {
				t.Fatalf("key %s processed out of order: %v", key, seq)
			}
		}
	}
	if len(seen) != 10 {
		t.Fatalf("expected 10 keys, got %d", len(seen))
	}
}

func TestShardForIsStable(t *testing.T) {
	p := NewKeyed(5, 1, nil)
	defer p.Close()
	first := p.ShardFor("abc")
	for range 10 {
		if got := p.ShardFor("abc"); got != first {
			t.Fatalf("same key must map to same shard: %d vs %d", got, first)
		}
	}
}
