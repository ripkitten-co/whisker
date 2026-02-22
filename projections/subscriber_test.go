package projections

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker/events"
)

func TestProjection_ImplementsSubscriber(t *testing.T) {
	p := New[OrderSummary](nil, "test")
	var _ Subscriber = p
}

func TestHandler_ImplementsSubscriber(t *testing.T) {
	h := NewHandler("test")
	var _ Subscriber = h
}

func TestHandler_ProcessSkipsUnregisteredEvents(t *testing.T) {
	h := NewHandler("test")
	called := false
	h.On("OrderCreated", func(ctx context.Context, evt events.Event) error {
		called = true
		return nil
	})

	err := h.Process(context.Background(), []events.Event{
		{Type: "UnknownEvent", StreamID: "stream-1"},
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("handler should not have been called for unregistered event")
	}
}

func TestHandler_ProcessCallsMatchingHandler(t *testing.T) {
	h := NewHandler("test")
	var received events.Event
	h.On("OrderCreated", func(ctx context.Context, evt events.Event) error {
		received = evt
		return nil
	})

	evt := events.Event{Type: "OrderCreated", StreamID: "stream-1"}
	err := h.Process(context.Background(), []events.Event{evt}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.StreamID != "stream-1" {
		t.Errorf("got stream %q, want %q", received.StreamID, "stream-1")
	}
}
