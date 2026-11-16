// Package pqueue implements a generic priority queue on top of a binary heap
// (container/heap).
//
// Data structure: a binary heap stored in a slice. The element with the
// highest priority (as defined by the less function) is always at index 0.
// Push and Pop are O(log n), Peek is O(1).
//
//	        [0]            children of i: 2i+1, 2i+2
//	      /     \          parent of i:   (i-1)/2
//	   [1]       [2]
//	  /   \     /   \
//	[3]   [4] [5]   [6]
//
// The queue is NOT safe for concurrent use; wrap it with a mutex (see the
// notification scheduler for an example).
package pqueue

import "container/heap"

// Queue is a priority queue. less(a, b) == true means a is served before b.
type Queue[T any] struct {
	h *innerHeap[T]
}

// New creates an empty queue ordered by less.
func New[T any](less func(a, b T) bool) *Queue[T] {
	return &Queue[T]{h: &innerHeap[T]{less: less}}
}

// Push adds an item. O(log n).
func (q *Queue[T]) Push(item T) { heap.Push(q.h, item) }

// Pop removes and returns the highest-priority item. O(log n).
func (q *Queue[T]) Pop() (T, bool) {
	if q.h.Len() == 0 {
		var zero T
		return zero, false
	}
	return heap.Pop(q.h).(T), true
}

// Peek returns the highest-priority item without removing it. O(1).
func (q *Queue[T]) Peek() (T, bool) {
	if q.h.Len() == 0 {
		var zero T
		return zero, false
	}
	return q.h.items[0], true
}

// Len returns the number of items.
func (q *Queue[T]) Len() int { return q.h.Len() }

// innerHeap adapts a slice to heap.Interface.
type innerHeap[T any] struct {
	items []T
	less  func(a, b T) bool
}

func (h *innerHeap[T]) Len() int           { return len(h.items) }
func (h *innerHeap[T]) Less(i, j int) bool { return h.less(h.items[i], h.items[j]) }
func (h *innerHeap[T]) Swap(i, j int)      { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *innerHeap[T]) Push(x any)         { h.items = append(h.items, x.(T)) }
func (h *innerHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	var zero T
	old[n-1] = zero // avoid a memory leak for pointer types
	h.items = old[:n-1]
	return item
}
