package kafka

import (
	"math/rand"
	"testing"
)

var tp0 = TopicPartition{Topic: "orders.v1", Partition: 0}

func TestCommitOnlyContiguousPrefix(t *testing.T) {
	tr := newOffsetTracker()
	for o := int64(5); o <= 8; o++ {
		tr.Track(tp0, o)
	}

	tr.Done(tp0, 6)
	tr.Done(tp0, 7)
	if got := tr.Committable(); len(got) != 0 {
		t.Fatalf("offset 5 still running, nothing should be committable, got %v", got)
	}

	tr.Done(tp0, 5)
	got := tr.Committable()
	if len(got) != 1 || got[0].Offset != 7 {
		t.Fatalf("want commit at 7, got %v", got)
	}
	if tr.InFlight() != 1 { // offset 8
		t.Fatalf("in flight = %d, want 1", tr.InFlight())
	}
	if again := tr.Committable(); len(again) != 0 {
		t.Fatalf("committable must be cleared after read, got %v", again)
	}
}

func TestPartitionsAreIndependent(t *testing.T) {
	tr := newOffsetTracker()
	tp1 := TopicPartition{Topic: "orders.v1", Partition: 1}
	tr.Track(tp0, 0)
	tr.Track(tp1, 100)
	tr.Done(tp1, 100)
	got := tr.Committable()
	if len(got) != 1 || got[0].TopicPartition != tp1 || got[0].Offset != 100 {
		t.Fatalf("got %v", got)
	}
}

// Property test: whatever order messages finish in, the final committed
// offset is the last one and nothing is ever committed past an unfinished one.
func TestRandomCompletionOrder(t *testing.T) {
	for run := 0; run < 50; run++ {
		tr := newOffsetTracker()
		const n = 100
		for o := int64(0); o < n; o++ {
			tr.Track(tp0, o)
		}
		finished := map[int64]bool{}
		var last int64 = -1
		for _, i := range rand.Perm(n) {
			tr.Done(tp0, int64(i))
			finished[int64(i)] = true
			for _, p := range tr.Committable() {
				for o := int64(0); o <= p.Offset; o++ {
					if !finished[o] {
						t.Fatalf("committed %d but %d not finished", p.Offset, o)
					}
				}
				last = p.Offset
			}
		}
		if last != n-1 {
			t.Fatalf("final commit %d, want %d", last, n-1)
		}
	}
}
