//go:build integration

package projections_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/projections"
)

type E2EOrder struct {
	ID     string `whisker:"id"`
	Status string
	Total  float64
}

func TestE2E_ProjectionAndHandler(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	proj := projections.New[E2EOrder](store, "e2e_orders")
	proj.On("OrderCreated", func(_ context.Context, evt events.Event, _ *E2EOrder) (*E2EOrder, error) {
		return &E2EOrder{ID: evt.StreamID, Status: "pending", Total: 99.99}, nil
	})
	proj.On("OrderPaid", func(_ context.Context, _ events.Event, state *E2EOrder) (*E2EOrder, error) {
		state.Status = "paid"
		return state, nil
	})

	var notifyCount atomic.Int32
	handler := projections.NewHandler("e2e_notifier")
	handler.On("OrderPaid", func(_ context.Context, _ events.Event) error {
		notifyCount.Add(1)
		return nil
	})

	daemon := projections.NewDaemon(store, projections.WithPollingInterval(100*time.Millisecond))
	daemon.Add(proj)
	daemon.Add(handler)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go daemon.Run(runCtx)

	es := events.New(store)
	err := es.Append(ctx, "order-42", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{}`)},
		{Type: "OrderPaid", Data: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	col := documents.Collection[E2EOrder](store, "e2e_orders")
	deadline := time.After(5 * time.Second)
	for {
		order, err := col.Load(ctx, "order-42")
		if err == nil && order.Status == "paid" && notifyCount.Load() == 1 {
			if order.Total != 99.99 {
				t.Fatalf("total: got %f, want 99.99", order.Total)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: order=%+v, notifyCount=%d, lastErr=%v", order, notifyCount.Load(), err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestE2E_ProjectionDeletesReadModel(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	proj := projections.New[E2EOrder](store, "e2e_delete_test")
	proj.On("OrderCreated", func(_ context.Context, evt events.Event, _ *E2EOrder) (*E2EOrder, error) {
		return &E2EOrder{ID: evt.StreamID, Status: "active"}, nil
	})
	proj.On("OrderCancelled", func(_ context.Context, _ events.Event, _ *E2EOrder) (*E2EOrder, error) {
		return nil, nil
	})

	daemon := projections.NewDaemon(store, projections.WithPollingInterval(100*time.Millisecond))
	daemon.Add(proj)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go daemon.Run(runCtx)

	es := events.New(store)
	err := es.Append(ctx, "order-99", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{}`)},
		{Type: "OrderCancelled", Data: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	col := documents.Collection[E2EOrder](store, "e2e_delete_test")
	cs := projections.NewCheckpointStore(store)
	deadline := time.After(5 * time.Second)
	for {
		exists, existsErr := col.Exists(ctx, "order-99")
		pos, _, posErr := cs.Load(ctx, "e2e_delete_test")

		if existsErr == nil && !exists && posErr == nil && pos > 0 {
			return
		}
		// while the table hasn't been created yet, Exists may return an error wrapping
		// whisker.ErrNotFound — keep polling
		if existsErr != nil && !errors.Is(existsErr, whisker.ErrNotFound) {
			// unexpected error — keep trying, the daemon might not have bootstrapped yet
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: exists=%v, existsErr=%v, pos=%d, posErr=%v", exists, existsErr, pos, posErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
