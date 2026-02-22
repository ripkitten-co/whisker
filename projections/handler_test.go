package projections

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker/events"
)

func TestHandler_OnRegistersHandler(t *testing.T) {
	h := NewHandler("email_notifier")

	h.On("OrderPaid", func(ctx context.Context, evt events.Event) error {
		return nil
	})

	if len(h.handlers) != 1 {
		t.Fatalf("got %d handlers, want 1", len(h.handlers))
	}
	if h.Name() != "email_notifier" {
		t.Errorf("got %q, want %q", h.Name(), "email_notifier")
	}
}

func TestHandler_EventTypes(t *testing.T) {
	h := NewHandler("notifier")
	h.On("A", func(ctx context.Context, evt events.Event) error { return nil })
	h.On("B", func(ctx context.Context, evt events.Event) error { return nil })

	types := h.EventTypes()
	if len(types) != 2 {
		t.Fatalf("got %d types, want 2", len(types))
	}
}
