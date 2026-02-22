package projections

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker/events"
)

type OrderSummary struct {
	ID     string `whisker:"id"`
	Status string
	Total  float64
}

func TestProjection_OnRegistersHandler(t *testing.T) {
	p := New[OrderSummary](nil, "order_summaries")

	p.On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		return &OrderSummary{ID: evt.StreamID, Status: "created"}, nil
	})
	p.On("OrderPaid", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		state.Status = "paid"
		return state, nil
	})

	if len(p.handlers) != 2 {
		t.Fatalf("got %d handlers, want 2", len(p.handlers))
	}
	if _, ok := p.handlers["OrderCreated"]; !ok {
		t.Error("missing OrderCreated handler")
	}
	if _, ok := p.handlers["OrderPaid"]; !ok {
		t.Error("missing OrderPaid handler")
	}
}

func TestProjection_Name(t *testing.T) {
	p := New[OrderSummary](nil, "order_summaries")
	if p.Name() != "order_summaries" {
		t.Errorf("got %q, want %q", p.Name(), "order_summaries")
	}
}

func TestProjection_EventTypes(t *testing.T) {
	p := New[OrderSummary](nil, "order_summaries")
	p.On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		return state, nil
	})
	p.On("OrderPaid", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		return state, nil
	})

	types := p.EventTypes()
	if len(types) != 2 {
		t.Fatalf("got %d types, want 2", len(types))
	}
}
