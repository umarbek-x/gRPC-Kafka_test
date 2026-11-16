// Package lru implements a thread-safe, generic Least-Recently-Used cache
// with optional per-entry TTL.
//
// Data structure: a hash map gives O(1) lookup, and a doubly linked list keeps
// entries ordered by recency (front = most recent). On every Get the entry is
// moved to the front; when the cache is full the entry at the back (least
// recently used) is evicted. All operations are O(1).
//
//	map[key] ──► *list.Element ──► entry{key, value, expiresAt}
//	list:  [most recent] ⇄ ... ⇄ [least recent]  ◄── evict from here
package lru

import (
	"container/list"
	"sync"
	"time"
)

type entry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time // zero = never expires
}

// Cache is safe for concurrent use.
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	ll       *list.List
	items    map[K]*list.Element
	now      func() time.Time // injectable clock for tests

	hits, misses, evictions uint64
}

// Stats is a snapshot of cache counters, useful for metrics.
type Stats struct {
	Hits, Misses, Evictions uint64
	Len                     int
}

// New creates a cache holding at most capacity entries. ttl <= 0 disables expiry.
func New[K comparable, V any](capacity int, ttl time.Duration) *Cache[K, V] {
	if capacity <= 0 {
		panic("lru: capacity must be > 0")
	}
	return &Cache[K, V]{
		capacity: capacity,
		ttl:      ttl,
		ll:       list.New(),
		items:    make(map[K]*list.Element, capacity),
		now:      time.Now,
	}
}

// Get returns the value and true if present and not expired.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	el, ok := c.items[key]
	if !ok {
		c.misses++
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if !e.expiresAt.IsZero() && c.now().After(e.expiresAt) {
		c.removeElement(el)
		c.misses++
		return zero, false
	}
	c.ll.MoveToFront(el)
	c.hits++
	return e.value, true
}

// Put inserts or replaces a value, evicting the LRU entry if needed.
func (c *Cache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var exp time.Time
	if c.ttl > 0 {
		exp = c.now().Add(c.ttl)
	}
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry[K, V])
		e.value, e.expiresAt = value, exp
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&entry[K, V]{key: key, value: value, expiresAt: exp})
	c.items[key] = el
	if c.ll.Len() > c.capacity {
		c.removeElement(c.ll.Back())
		c.evictions++
	}
}

// Contains reports presence without updating recency.
func (c *Cache[K, V]) Contains(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return false
	}
	e := el.Value.(*entry[K, V])
	return e.expiresAt.IsZero() || !c.now().After(e.expiresAt)
}

// Delete removes a key if present.
func (c *Cache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeElement(el)
	}
}

// Len returns the number of entries (including not-yet-collected expired ones).
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Stats returns counters.
func (c *Cache[K, V]) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Hits: c.hits, Misses: c.misses, Evictions: c.evictions, Len: c.ll.Len()}
}

func (c *Cache[K, V]) removeElement(el *list.Element) {
	c.ll.Remove(el)
	delete(c.items, el.Value.(*entry[K, V]).key)
}
