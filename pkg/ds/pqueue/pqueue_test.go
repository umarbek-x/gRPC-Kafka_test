package pqueue

import (
	"math/rand"
	"sort"
	"testing"
)

func TestMinHeapOrder(t *testing.T) {
	q := New(func(a, b int) bool { return a < b })
	for _, v := range rand.Perm(100) {
		q.Push(v)
	}
	for want := 0; want < 100; want++ {
		got, ok := q.Pop()
		if !ok || got != want {
			t.Fatalf("pop = %d,%v want %d", got, ok, want)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("expected empty")
	}
}

type job struct {
	priority int
	seq      int
}

// Higher priority first; FIFO among equal priorities (stable via seq).
func TestPriorityWithFIFOTieBreak(t *testing.T) {
	q := New(func(a, b job) bool {
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		return a.seq < b.seq
	})
	jobs := []job{{1, 0}, {3, 1}, {1, 2}, {3, 3}, {2, 4}}
	for _, j := range jobs {
		q.Push(j)
	}
	want := append([]job(nil), jobs...)
	sort.SliceStable(want, func(i, j int) bool { return want[i].priority > want[j].priority })

	if top, _ := q.Peek(); top != want[0] {
		t.Fatalf("peek = %+v want %+v", top, want[0])
	}
	for i := range want {
		got, _ := q.Pop()
		if got != want[i] {
			t.Fatalf("pop #%d = %+v want %+v", i, got, want[i])
		}
	}
}
