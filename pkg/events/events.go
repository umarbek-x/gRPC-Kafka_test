// Package events defines topics, headers and the (de)serialization of
// integration events shared by all services.
package events

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/observability"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/outbox"
)

// Topics. The ".v1" suffix allows a breaking schema change to go to a new
// topic while old consumers keep reading the old one.
const (
	TopicOrders   = "orders.v1"
	TopicPayments = "payments.v1"
)

// Kafka header names. Headers let consumers route/filter WITHOUT decoding the payload.
const (
	HeaderEventType     = "event-type"
	HeaderEventID       = "event-id"
	HeaderCorrelationID = "correlation-id"
	HeaderContentType   = "content-type"
	ContentTypeJSON     = "application/json"
)

// Event is any generated event message (they all have GetMetadata()).
type Event interface {
	proto.Message
	GetMetadata() *eventsv1.EventMetadata
}

// NewMetadata builds metadata for msg using the correlation ID found in ctx.
func NewMetadata(ctx context.Context, producer string, msg proto.Message) *eventsv1.EventMetadata {
	return &eventsv1.EventMetadata{
		EventId:       uuid.NewString(),
		EventType:     TypeName(msg),
		OccurredAt:    timestamppb.New(time.Now().UTC()),
		CorrelationId: observability.CorrelationID(ctx),
		Producer:      producer,
	}
}

// TypeName returns the fully-qualified proto name, e.g. "orderflow.events.v1.OrderCreated".
func TypeName(msg proto.Message) string {
	return string(msg.ProtoReflect().Descriptor().FullName())
}

// ToOutbox serializes an event into an outbox message.
func ToOutbox(topic, key string, evt Event) (outbox.Message, error) {
	md := evt.GetMetadata()
	if md == nil || md.GetEventId() == "" {
		return outbox.Message{}, fmt.Errorf("event %s has no metadata", TypeName(evt))
	}
	payload, err := protojson.Marshal(evt)
	if err != nil {
		return outbox.Message{}, fmt.Errorf("marshal %s: %w", TypeName(evt), err)
	}
	return outbox.Message{
		EventID:   md.GetEventId(),
		Topic:     topic,
		Key:       key,
		EventType: md.GetEventType(),
		Payload:   payload,
		Headers: map[string]string{
			HeaderEventType:     md.GetEventType(),
			HeaderEventID:       md.GetEventId(),
			HeaderCorrelationID: md.GetCorrelationId(),
			HeaderContentType:   ContentTypeJSON,
		},
	}, nil
}

// Decode turns a Kafka message back into a concrete event using the
// event-type header and the protobuf global registry, so adding a new event
// type needs no changes here.
func Decode(msg kafka.Message) (Event, error) {
	typ := msg.Headers[HeaderEventType]
	if typ == "" {
		return nil, fmt.Errorf("missing %s header", HeaderEventType)
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typ))
	if err != nil {
		return nil, fmt.Errorf("unknown event type %q: %w", typ, err)
	}
	m := mt.New().Interface()
	// DiscardUnknown: a newer producer may add fields we don't know yet
	// (forward compatibility). Don't fail on them.
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(msg.Value, m); err != nil {
		return nil, fmt.Errorf("decode %s: %w", typ, err)
	}
	evt, ok := m.(Event)
	if !ok {
		return nil, fmt.Errorf("type %s is not an event", typ)
	}
	return evt, nil
}

// ContextFrom returns ctx enriched with the correlation ID of the message,
// so logs of the consumer can be joined with logs of the producer.
func ContextFrom(ctx context.Context, msg kafka.Message) context.Context {
	if id := msg.Headers[HeaderCorrelationID]; id != "" {
		return observability.WithCorrelationID(ctx, id)
	}
	ctx, _ = observability.EnsureCorrelationID(ctx)
	return ctx
}
