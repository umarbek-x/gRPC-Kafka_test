package kafka

import "sync"

// TopicPartition identifies one partition of one topic.
type TopicPartition struct {
	Topic     string
	Partition int
}

// Position is the offset that is safe to commit for a partition.
type Position struct {
	TopicPartition
	Offset int64 // offset of the last fully processed message
}

// offsetTracker solves the hardest problem of concurrent Kafka consumption:
// WHICH OFFSET IS SAFE TO COMMIT?
//
// Kafka stores one committed offset per partition, meaning "everything before
// this has been processed". With a worker pool, messages from one partition
// finish OUT OF ORDER (offset 7 may finish before offset 5 because they have
// different keys and run on different workers). If we committed 7 and then
// crashed, offset 5 would never be processed: data loss.
//
// So per partition we keep the offsets in fetch order and only advance the
// commit position across a CONTIGUOUS run of completed offsets ("low
// watermark"):
//
//	fetched:  5  6  7  8
//	done:     .  ✓  ✓  .      -> nothing committable (5 still running)
//	done:     ✓  ✓  ✓  .      -> committable = 7
//
// Data structures: a FIFO slice of pending offsets (fetch order) + a set of
// completed offsets. Each Done() is amortized O(1).
type offsetTracker struct {
	mu    sync.Mutex
	parts map[TopicPartition]*partitionState
}

type partitionState struct {
	pending []int64            // offsets in fetch order, not yet committable
	done    map[int64]struct{} // completed but blocked behind a smaller offset
	ready   int64              // highest committable offset, -1 if none new
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{parts: make(map[TopicPartition]*partitionState)}
}

// Track must be called in fetch order, before the message is processed.
func (t *offsetTracker) Track(tp TopicPartition, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ps, ok := t.parts[tp]
	if !ok {
		ps = &partitionState{done: make(map[int64]struct{}), ready: -1}
		t.parts[tp] = ps
	}
	ps.pending = append(ps.pending, offset)
}

// Done marks a message as fully processed (successfully or sent to DLQ).
func (t *offsetTracker) Done(tp TopicPartition, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ps, ok := t.parts[tp]
	if !ok {
		return
	}
	ps.done[offset] = struct{}{}
	// Advance the watermark while the head of the queue is complete.
	for len(ps.pending) > 0 {
		head := ps.pending[0]
		if _, finished := ps.done[head]; !finished {
			break
		}
		delete(ps.done, head)
		ps.pending = ps.pending[1:]
		ps.ready = head
	}
}

// Committable returns and clears the new commit positions.
func (t *offsetTracker) Committable() []Position {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []Position
	for tp, ps := range t.parts {
		if ps.ready >= 0 {
			out = append(out, Position{TopicPartition: tp, Offset: ps.ready})
			ps.ready = -1
		}
	}
	return out
}

// InFlight returns the number of tracked-but-uncommittable messages.
func (t *offsetTracker) InFlight() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, ps := range t.parts {
		n += len(ps.pending)
	}
	return n
}
