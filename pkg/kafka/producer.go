// Package kafka is a thin, production-minded layer over segmentio/kafka-go.
//
// Producer: synchronous, acks=all, key-hash partitioning (same key -> same
// partition -> ordered).
//
// Consumer: consumer group + keyed worker pool + safe offset commits +
// retries with backoff + dead-letter queue. See consumer.go.
package kafka

import (
	"context"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Message is our transport-neutral message type. Services never import
// kafka-go directly, which keeps the client library swappable (e.g. franz-go).
type Message struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	Partition int
	Offset    int64
	Time      time.Time
}

// Producer publishes messages.
type Producer struct {
	w *kafkago.Writer
}

// NewProducer creates a producer.
//
//   - RequiredAcks=RequireAll: the leader waits until all in-sync replicas
//     have the message. Slowest but no data loss if a broker dies.
//   - Murmur2Balancer: same partitioning as the Java client, so a key lands
//     on the same partition no matter which language produced it.
//   - The Writer batches internally; BatchTimeout bounds the extra latency.
func NewProducer(brokers []string) *Producer {
	return &Producer{w: &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Balancer:               &kafkago.Murmur2Balancer{},
		RequiredAcks:           kafkago.RequireAll,
		BatchTimeout:           10 * time.Millisecond,
		MaxAttempts:            5,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false, // topics are infrastructure, created explicitly
	}}
}

// Publish writes messages synchronously and returns when Kafka acknowledged them.
func (p *Producer) Publish(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}
	kms := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		kms[i] = kafkago.Message{Topic: m.Topic, Key: []byte(m.Key), Value: m.Value, Headers: toKafkaHeaders(m.Headers)}
	}
	if err := p.w.WriteMessages(ctx, kms...); err != nil {
		return fmt.Errorf("kafka publish: %w", err)
	}
	return nil
}

// Close flushes pending writes.
func (p *Producer) Close() error { return p.w.Close() }

func toKafkaHeaders(h map[string]string) []kafkago.Header {
	out := make([]kafkago.Header, 0, len(h))
	for k, v := range h {
		out = append(out, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return out
}

func fromKafkaHeaders(hs []kafkago.Header) map[string]string {
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}
