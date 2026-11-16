// Package notification implements the notification-service: it listens to
// order and payment events and "sends" messages to customers.
//
// It is stateless (no database) and shows three in-memory data structures
// working together:
//
//	priority queue  — important notifications jump the line
//	token bucket    — respect the email provider's rate limit
//	LRU cache       — skip events we have already handled
//	ring buffer     — keep the last N notifications for a debug endpoint
package notification

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/umarbek-x/gRPC-Kafka_test/pkg/ds/pqueue"
)

// Priority: higher is sent first.
type Priority int

const (
	PriorityLow    Priority = 1 // marketing-like: "order received"
	PriorityNormal Priority = 5 // "payment confirmed"
	PriorityHigh   Priority = 9 // customer must act: "payment failed"
)

type Notification struct {
	EventID    string    `json:"event_id"`
	CustomerID string    `json:"customer_id"`
	OrderID    string    `json:"order_id"`
	Subject    string    `json:"subject"`
	Body       string    `json:"body"`
	Priority   Priority  `json:"priority"`
	SentAt     time.Time `json:"sent_at"`
}

// Sender delivers a notification (email, SMS, push...).
type Sender interface {
	Send(ctx context.Context, n Notification) error
}

// Limiter is satisfied by *ratelimit.TokenBucket.
type Limiter interface {
	Wait(ctx context.Context) error
}

type job struct {
	n      Notification
	seq    uint64          // arrival order, for FIFO among equal priorities
	ctx    context.Context // submitter's context
	result chan error      // buffered(1): the dispatcher never blocks on it
}

// ErrSchedulerStopped is returned to submitters when the scheduler shuts down.
var ErrSchedulerStopped = errors.New("notification scheduler stopped")

// Scheduler sends notifications in priority order, never faster than the
// rate limit allows.
//
//	Submit ─► [priority queue] ─► dispatcher goroutines ─► limiter ─► Sender
//	   ▲                                                              │
//	   └────────────────────── result (error or nil) ◄────────────────┘
//
// Submit BLOCKS until its notification was actually sent. That matters: the
// Kafka consumer commits an offset only after the handler returns, so a
// notification that is still waiting in memory is never "acknowledged" to
// Kafka. If the process crashes, Kafka redelivers it. Nothing is lost.
type Scheduler struct {
	mu      sync.Mutex
	queue   *pqueue.Queue[*job]
	seq     uint64
	wake    chan struct{} // signals "queue is not empty"
	sender  Sender
	limit   Limiter
	stopped chan struct{} // closed when Run has returned
}

func NewScheduler(sender Sender, limit Limiter) *Scheduler {
	return &Scheduler{
		queue: pqueue.New(func(a, b *job) bool {
			if a.n.Priority != b.n.Priority {
				return a.n.Priority > b.n.Priority
			}
			return a.seq < b.seq
		}),
		wake:    make(chan struct{}, 1),
		sender:  sender,
		limit:   limit,
		stopped: make(chan struct{}),
	}
}

// Submit queues n and waits until it is sent (or ctx is cancelled).
func (s *Scheduler) Submit(ctx context.Context, n Notification) error {
	j := &job{n: n, ctx: ctx, result: make(chan error, 1)}
	s.mu.Lock()
	s.seq++
	j.seq = s.seq
	s.queue.Push(j)
	s.mu.Unlock()
	s.signal()

	select {
	case err := <-j.result:
		return err
	case <-ctx.Done():
		return ctx.Err() // the dispatcher will skip this job when it pops it
	case <-s.stopped:
		return ErrSchedulerStopped // nobody will ever pop the job
	}
}

// signal wakes one dispatcher without ever blocking: if a wake-up is already
// pending, that one is enough.
func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) pop() (*job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.queue.Pop()
	if ok && s.queue.Len() > 0 {
		// More work remains: pass the wake-up on to another dispatcher.
		// Without this, a single buffered signal could leave jobs waiting
		// while dispatchers sleep (a "lost wake-up").
		s.signal()
	}
	return j, ok
}

// Pending returns the queue length (for metrics).
func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queue.Len()
}

// Run starts `dispatchers` goroutines and blocks until ctx is cancelled and
// they have all exited. Jobs still queued at that point fail with
// ErrSchedulerStopped, so their Kafka messages are redelivered later.
func (s *Scheduler) Run(ctx context.Context, dispatchers int) error {
	if dispatchers <= 0 {
		dispatchers = 1
	}
	var wg sync.WaitGroup
	wg.Add(dispatchers)
	for i := 0; i < dispatchers; i++ {
		go func() {
			defer wg.Done()
			s.dispatch(ctx)
		}()
	}
	wg.Wait()
	defer close(s.stopped)

	for {
		j, ok := s.pop()
		if !ok {
			return nil
		}
		j.result <- ErrSchedulerStopped
	}
}

func (s *Scheduler) dispatch(ctx context.Context) {
	for {
		j, ok := s.pop()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
				continue
			}
		}
		if j.ctx.Err() != nil {
			continue // submitter gave up; don't waste a rate-limit token
		}
		// Take the job FIRST, then wait for a token: the highest-priority job
		// present at pop time is the one that gets the next token.
		if err := s.limit.Wait(ctx); err != nil {
			j.result <- ErrSchedulerStopped
			return
		}
		j.result <- s.sender.Send(j.ctx, j.n)
	}
}
