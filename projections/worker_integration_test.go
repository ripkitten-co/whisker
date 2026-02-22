//go:build integration

package projections_test

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/projections"
)

type OrderSummary struct {
	ID     string `whisker:"id"`
	Status string
	Total  float64
}

func TestWorker_ProcessesBatch(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "order-1", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"id":"order-1","status":"created","total":0}`)},
		{Type: "OrderPaid", Data: []byte(`{"amount":99.95}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var processed []string
	proj := projections.New[OrderSummary](store, "order_summaries")
	proj.On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		processed = append(processed, evt.Type)
		return &OrderSummary{ID: evt.StreamID, Status: "created"}, nil
	})
	proj.On("OrderPaid", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		processed = append(processed, evt.Type)
		state.Status = "paid"
		return state, nil
	})

	w := projections.NewWorker(store, proj)
	if err := w.ProcessBatch(ctx); err != nil {
		t.Fatalf("process batch: %v", err)
	}

	if len(processed) != 2 {
		t.Fatalf("processed %d events, want 2", len(processed))
	}
	if processed[0] != "OrderCreated" {
		t.Errorf("processed[0]: got %q, want %q", processed[0], "OrderCreated")
	}
	if processed[1] != "OrderPaid" {
		t.Errorf("processed[1]: got %q, want %q", processed[1], "OrderPaid")
	}

	cs := projections.NewCheckpointStore(store)
	pos, _, err := cs.Load(ctx, "order_summaries")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if pos <= 0 {
		t.Errorf("checkpoint position: got %d, want > 0", pos)
	}
}

func TestWorker_SkipsDeadLetterStatus(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "order-2", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"id":"order-2"}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var processed []string
	proj := projections.New[OrderSummary](store, "dead_letter_proj")
	proj.On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		processed = append(processed, evt.Type)
		return &OrderSummary{ID: evt.StreamID}, nil
	})

	cs := projections.NewCheckpointStore(store)
	if err := cs.Save(ctx, "dead_letter_proj", 0); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	if err := cs.SetStatus(ctx, "dead_letter_proj", "dead_letter"); err != nil {
		t.Fatalf("set status: %v", err)
	}

	w := projections.NewWorker(store, proj)
	if err := w.ProcessBatch(ctx); err != nil {
		t.Fatalf("process batch: %v", err)
	}

	if len(processed) != 0 {
		t.Errorf("processed %d events, want 0 (dead_letter should skip)", len(processed))
	}
}

func TestWorker_FiltersByEventType(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "order-3", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"id":"order-3"}`)},
		{Type: "OrderShipped", Data: []byte(`{}`)},
		{Type: "OrderPaid", Data: []byte(`{"amount":50}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var processed []string
	proj := projections.New[OrderSummary](store, "filter_proj")
	proj.On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		processed = append(processed, evt.Type)
		return &OrderSummary{ID: evt.StreamID, Status: "created"}, nil
	})

	w := projections.NewWorker(store, proj)
	if err := w.ProcessBatch(ctx); err != nil {
		t.Fatalf("process batch: %v", err)
	}

	if len(processed) != 1 {
		t.Fatalf("processed %d events, want 1", len(processed))
	}
	if processed[0] != "OrderCreated" {
		t.Errorf("processed[0]: got %q, want %q", processed[0], "OrderCreated")
	}

	cs := projections.NewCheckpointStore(store)
	pos, _, err := cs.Load(ctx, "filter_proj")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}

	allEvts, err := es.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	lastPos := allEvts[len(allEvts)-1].GlobalPosition
	if pos != lastPos {
		t.Errorf("checkpoint position: got %d, want %d (should advance past all events)", pos, lastPos)
	}
}

func TestWorker_AdvisoryLock(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	proj := projections.New[OrderSummary](store, "lock_test_proj")
	w := projections.NewWorker(store, proj)

	acquired, err := w.TryAcquireLock(ctx)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}
	if !acquired {
		t.Fatal("expected to acquire lock")
	}

	if err := w.ReleaseLock(ctx); err != nil {
		t.Fatalf("release lock: %v", err)
	}
}
