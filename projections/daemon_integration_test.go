//go:build integration

package projections_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/projections"
)

func TestDaemon_RunProcessesEvents(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "order-d1", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"id":"order-d1","status":"created","total":0}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var count atomic.Int64
	proj := projections.New[OrderSummary](store, "daemon_run_proj")
	proj.On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		count.Add(1)
		return &OrderSummary{ID: evt.StreamID, Status: "created"}, nil
	})

	daemon := projections.NewDaemon(store, projections.WithPollingInterval(100*time.Millisecond))
	daemon.Add(proj)

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	go daemon.Run(runCtx)

	deadline := time.After(2 * time.Second)
	for {
		if count.Load() >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for event processing, count=%d", count.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestDaemon_SideEffectHandler(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "order-d2", 0, []events.Event{
		{Type: "OrderPaid", Data: []byte(`{"amount":49.99}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var fired atomic.Bool
	handler := projections.NewHandler("daemon_side_effect")
	handler.On("OrderPaid", func(ctx context.Context, evt events.Event) error {
		fired.Store(true)
		return nil
	})

	daemon := projections.NewDaemon(store, projections.WithPollingInterval(100*time.Millisecond))
	daemon.Add(handler)

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	go daemon.Run(runCtx)

	deadline := time.After(2 * time.Second)
	for {
		if fired.Load() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for handler to fire")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestDaemon_Rebuild(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "order-d3", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"id":"order-d3","status":"created","total":0}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	var count atomic.Int64
	proj := projections.New[OrderSummary](store, "daemon_rebuild_proj")
	proj.On("OrderCreated", func(ctx context.Context, evt events.Event, state *OrderSummary) (*OrderSummary, error) {
		count.Add(1)
		return &OrderSummary{ID: evt.StreamID, Status: "created"}, nil
	})

	daemon := projections.NewDaemon(store, projections.WithPollingInterval(100*time.Millisecond))
	daemon.Add(proj)

	// run until event is processed
	runCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	go daemon.Run(runCtx)

	deadline := time.After(2 * time.Second)
	for {
		if count.Load() >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for initial processing, count=%d", count.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	// give goroutines time to stop
	time.Sleep(200 * time.Millisecond)

	count.Store(0)

	if err := daemon.Rebuild(ctx, "daemon_rebuild_proj"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	if count.Load() != 1 {
		t.Errorf("count after rebuild: got %d, want 1", count.Load())
	}

	// verify status is back to running
	cs := projections.NewCheckpointStore(store)
	_, status, err := cs.Load(ctx, "daemon_rebuild_proj")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if status != "running" {
		t.Errorf("status after rebuild: got %q, want %q", status, "running")
	}
}
