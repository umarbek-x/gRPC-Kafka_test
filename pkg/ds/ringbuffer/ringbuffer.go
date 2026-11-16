// Package ringbuffer implements a fixed-size, thread-safe circular buffer that
// keeps the N most recent items. Useful for "last N events" debug endpoints,
// sliding windows and bounded in-memory history.
//
// Data structure: a fixed slice plus a write index that wraps around. When
// the buffer is full, new writes overwrite the oldest item. Memory stays
// constant no matter how many items are written.
//
//	cap=4, after writing a b c d e f:
//	buf:  [e][f][c][d]
//	            ^ head = next write position = oldest item
package ringbuffer

import "sync"

type Buffer[T any] struct {
	mu   sync.RWMutex
	buf  []T
	head int // next write position
	size int // number of valid items
}

func New[T any](capacity int) *Buffer[T] {
	if capacity <= 0 {
		panic("ringbuffer: capacity must be > 0")
	}
	return &Buffer[T]{buf: make([]T, capacity)}
}

// Push appends an item, overwriting the oldest when full. O(1).
func (b *Buffer[T]) Push(v T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf[b.head] = v
	b.head = (b.head + 1) % len(b.buf)
	if b.size < len(b.buf) {
		b.size++
	}
}

// Snapshot returns a copy of the items ordered oldest → newest.
func (b *Buffer[T]) Snapshot() []T {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]T, 0, b.size)
	start := (b.head - b.size + len(b.buf)) % len(b.buf)
	for i := 0; i < b.size; i++ {
		out = append(out, b.buf[(start+i)%len(b.buf)])
	}
	return out
}

// Len returns the number of stored items.
func (b *Buffer[T]) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.size
}
