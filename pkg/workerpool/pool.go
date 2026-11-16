// Package workerpool provides two bounded goroutine pools:
//
//   - Pool: N workers share one task queue. Use it for independent jobs where
//     order does not matter (e.g. firing load-test requests).
//
//   - KeyedPool: N workers, each with its OWN queue. A task is routed to a
//     worker by hashing its key, so all tasks with the same key run
//     sequentially, in submission order, on the same worker, while different
//     keys run in parallel. This is exactly what a Kafka consumer needs:
//     events for one order must be applied in order, but events for different
//     orders can be processed concurrently.
//
// Why bound the pool? Spawning one goroutine per task is cheap, but unbounded
// concurrency means unbounded memory, DB connections and downstream load.
// A bounded queue gives BACKPRESSURE: when workers are busy, Submit blocks,
// which naturally slows down the producer (e.g. stops fetching from Kafka).
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
)

// ErrClosed is returned when submitting to a closed pool.
var ErrClosed = errors.New("workerpool: closed")

// PanicHandler is called when a task panics. The worker survives.
type PanicHandler func(recovered any)

// Pool is a fixed-size pool of workers consuming a shared queue.
type Pool struct {
	tasks   chan func()
	wg      sync.WaitGroup
	mu      sync.RWMutex // guards closed + sends on tasks
	closed  bool
	onPanic PanicHandler
}

// New starts `workers` goroutines with a queue of `queueSize` pending tasks.
func New(workers, queueSize int, onPanic PanicHandler) *Pool {
	if workers <= 0 {
		panic("workerpool: workers must be > 0")
	}
	p := &Pool{tasks: make(chan func(), queueSize), onPanic: onPanic}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	// range over a channel ends when the channel is closed AND drained,
	// so Close() lets queued tasks finish (graceful drain).
	for task := range p.tasks {
		runSafely(task, p.onPanic)
	}
}

// Submit enqueues a task, blocking while the queue is full (backpressure).
// It returns ctx.Err() if ctx is cancelled first.
func (p *Pool) Submit(ctx context.Context, task func()) error {
	// Read lock: many Submits can run concurrently, but Close (write lock)
	// waits for them, so we never send on a closed channel (which panics).
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.tasks <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting tasks and waits for queued + running tasks to finish.
func (p *Pool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.tasks)
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// KeyedPool routes tasks with the same key to the same worker (shard).
type KeyedPool struct {
	shards  []chan func()
	wg      sync.WaitGroup
	mu      sync.RWMutex
	closed  bool
	onPanic PanicHandler
}

// NewKeyed starts `workers` shards, each with its own queue of `queueSize`.
func NewKeyed(workers, queueSize int, onPanic PanicHandler) *KeyedPool {
	if workers <= 0 {
		panic("workerpool: workers must be > 0")
	}
	p := &KeyedPool{shards: make([]chan func(), workers), onPanic: onPanic}
	p.wg.Add(workers)
	for i := range p.shards {
		ch := make(chan func(), queueSize)
		p.shards[i] = ch
		go func() {
			defer p.wg.Done()
			for task := range ch {
				runSafely(task, p.onPanic)
			}
		}()
	}
	return p
}

// ShardFor returns the shard index for a key (exported for tests/metrics).
func (p *KeyedPool) ShardFor(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(p.shards)))
}

// Submit enqueues a task on the shard owning key. Blocks when that shard's
// queue is full.
func (p *KeyedPool) Submit(ctx context.Context, key string, task func()) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.shards[p.ShardFor(key)] <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting tasks and waits until all queued tasks are done.
func (p *KeyedPool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		for _, ch := range p.shards {
			close(ch)
		}
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func runSafely(task func(), onPanic PanicHandler) {
	defer func() {
		if r := recover(); r != nil {
			if onPanic != nil {
				onPanic(r)
				return
			}
			// No handler: print and keep the worker alive anyway.
			fmt.Printf("workerpool: task panicked: %v\n", r)
		}
	}()
	task()
}
