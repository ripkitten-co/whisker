//go:build integration

package projections_test

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/testutil"
	"github.com/ripkitten-co/whisker/projections"
)

func setupStore(t *testing.T) *whisker.Store {
	t.Helper()
	connStr := testutil.SetupPostgres(t)
	store, err := whisker.New(context.Background(), connStr)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestCheckpoint_ReadWriteRoundTrip(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	cs := projections.NewCheckpointStore(store)

	pos, status, err := cs.Load(ctx, "order_totals")
	if err != nil {
		t.Fatalf("initial load: %v", err)
	}
	if pos != 0 {
		t.Errorf("initial position: got %d, want 0", pos)
	}
	if status != "running" {
		t.Errorf("initial status: got %q, want %q", status, "running")
	}

	if err := cs.Save(ctx, "order_totals", 42); err != nil {
		t.Fatalf("save: %v", err)
	}

	pos, status, err = cs.Load(ctx, "order_totals")
	if err != nil {
		t.Fatalf("load after save: %v", err)
	}
	if pos != 42 {
		t.Errorf("position after save: got %d, want 42", pos)
	}
	if status != "running" {
		t.Errorf("status after save: got %q, want %q", status, "running")
	}
}

func TestCheckpoint_SetStatus(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	cs := projections.NewCheckpointStore(store)

	if err := cs.Save(ctx, "invoice_totals", 10); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := cs.SetStatus(ctx, "invoice_totals", "dead_letter"); err != nil {
		t.Fatalf("set status: %v", err)
	}

	pos, status, err := cs.Load(ctx, "invoice_totals")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if pos != 10 {
		t.Errorf("position: got %d, want 10", pos)
	}
	if status != "dead_letter" {
		t.Errorf("status: got %q, want %q", status, "dead_letter")
	}
}

func TestCheckpoint_Reset(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	cs := projections.NewCheckpointStore(store)

	if err := cs.Save(ctx, "shipping_view", 99); err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := cs.Reset(ctx, "shipping_view"); err != nil {
		t.Fatalf("reset: %v", err)
	}

	pos, status, err := cs.Load(ctx, "shipping_view")
	if err != nil {
		t.Fatalf("load after reset: %v", err)
	}
	if pos != 0 {
		t.Errorf("position after reset: got %d, want 0", pos)
	}
	if status != "rebuilding" {
		t.Errorf("status after reset: got %q, want %q", status, "rebuilding")
	}
}
