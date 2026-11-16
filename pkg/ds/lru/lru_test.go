package lru

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	c := New[string, int](2, 0)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Get("a")    // "a" is now most recent, "b" is LRU
	c.Put("c", 3) // evicts "b"

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	for _, k := range []string{"a", "c"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("expected %s to be present", k)
		}
	}
	if s := c.Stats(); s.Evictions != 1 {
		t.Fatalf("evictions = %d, want 1", s.Evictions)
	}
}

func TestPutUpdatesExisting(t *testing.T) {
	c := New[string, int](2, 0)
	c.Put("a", 1)
	c.Put("a", 2)
	if v, _ := c.Get("a"); v != 2 {
		t.Fatalf("got %d, want 2", v)
	}
	if c.Len() != 1 {
		t.Fatalf("len = %d, want 1", c.Len())
	}
}

func TestTTLExpiry(t *testing.T) {
	now := time.Unix(0, 0)
	c := New[string, int](10, time.Minute)
	c.now = func() time.Time { return now }

	c.Put("a", 1)
	now = now.Add(59 * time.Second)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("should still be valid")
	}
	now = now.Add(2 * time.Second)
	if _, ok := c.Get("a"); ok {
		t.Fatal("should be expired")
	}
	if c.Len() != 0 {
		t.Fatal("expired entry should be removed on access")
	}
}

func TestDelete(t *testing.T) {
	c := New[int, string](4, 0)
	c.Put(1, "x")
	c.Delete(1)
	if c.Contains(1) {
		t.Fatal("expected deleted")
	}
}

// Run with -race to prove the cache is safe for concurrent use.
func TestConcurrentAccess(t *testing.T) {
	c := New[string, int](100, 0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				k := fmt.Sprintf("k%d", (g*i)%150)
				c.Put(k, i)
				c.Get(k)
			}
		}(g)
	}
	wg.Wait()
	if c.Len() > 100 {
		t.Fatalf("len %d exceeds capacity", c.Len())
	}
}

func BenchmarkGetPut(b *testing.B) {
	c := New[int, int](1024, 0)
	for i := 0; i < b.N; i++ {
		c.Put(i%2048, i)
		c.Get(i % 1024)
	}
}
