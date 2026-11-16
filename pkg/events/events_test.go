package events

import (
	"context"
	"testing"

	eventsv1 "github.com/umarbek-x/gRPC-Kafka_test/gen/go/events/v1"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/kafka"
	"github.com/umarbek-x/gRPC-Kafka_test/pkg/observability"
)

func TestOutboxRoundTrip(t *testing.T) {
	ctx := observability.WithCorrelationID(context.Background(), "corr-1")
	in := &eventsv1.OrderCreated{OrderId: "o-1", CustomerId: "c-1", TotalMinor: 1500, Currency: "USD"}
	in.Metadata = NewMetadata(ctx, "test", in)

	om, err := ToOutbox(TopicOrders, in.OrderId, in)
	if err != nil {
		t.Fatal(err)
	}
	if om.Headers[HeaderEventType] != "orderflow.events.v1.OrderCreated" {
		t.Fatalf("event type header = %q", om.Headers[HeaderEventType])
	}
	if om.Headers[HeaderCorrelationID] != "corr-1" {
		t.Fatal("correlation id not propagated")
	}

	evt, err := Decode(kafka.Message{Value: om.Payload, Headers: om.Headers})
	if err != nil {
		t.Fatal(err)
	}
	out, ok := evt.(*eventsv1.OrderCreated)
	if !ok {
		t.Fatalf("decoded type %T", evt)
	}
	if out.OrderId != "o-1" || out.TotalMinor != 1500 || out.GetMetadata().GetEventId() != in.Metadata.EventId {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestDecodeRejectsUnknownType(t *testing.T) {
	_, err := Decode(kafka.Message{Value: []byte(`{}`), Headers: map[string]string{HeaderEventType: "nope.Unknown"}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecodeIgnoresUnknownFields(t *testing.T) {
	// A newer producer added "gift_wrap"; an older consumer must still work.
	payload := []byte(`{"orderId":"o-9","giftWrap":true,"metadata":{"eventId":"e"}}`)
	evt, err := Decode(kafka.Message{Value: payload, Headers: map[string]string{HeaderEventType: "orderflow.events.v1.OrderCreated"}})
	if err != nil {
		t.Fatal(err)
	}
	if evt.(*eventsv1.OrderCreated).OrderId != "o-9" {
		t.Fatal("wrong order id")
	}
}
