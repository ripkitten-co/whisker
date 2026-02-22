//go:build integration

package projections_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ripkitten-co/whisker/projections"
)

func jsonEqual(a, b []byte) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	ra, _ := json.Marshal(va)
	rb, _ := json.Marshal(vb)
	return string(ra) == string(rb)
}

func TestProcessingStore_UpsertAndLoad(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	ps := projections.NewProcessingStoreFromBackend(store, "ps_test_items")

	data := []byte(`{"name":"widget","price":42}`)
	if err := ps.UpsertState(ctx, "", "item-1", data, 0); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, version, err := ps.LoadState(ctx, "", "item-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if version != 1 {
		t.Errorf("version: got %d, want 1", version)
	}
	if !jsonEqual(got, data) {
		t.Errorf("data: got %s, want %s", got, data)
	}
}

func TestProcessingStore_UpsertUpdatesExisting(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	ps := projections.NewProcessingStoreFromBackend(store, "ps_test_updates")

	first := []byte(`{"count":1}`)
	if err := ps.UpsertState(ctx, "", "counter-1", first, 0); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	second := []byte(`{"count":2}`)
	if err := ps.UpsertState(ctx, "", "counter-1", second, 1); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	got, version, err := ps.LoadState(ctx, "", "counter-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if version != 2 {
		t.Errorf("version: got %d, want 2", version)
	}
	if !jsonEqual(got, second) {
		t.Errorf("data: got %s, want %s", got, second)
	}
}

func TestProcessingStore_Delete(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	ps := projections.NewProcessingStoreFromBackend(store, "ps_test_delete")

	data := []byte(`{"temp":true}`)
	if err := ps.UpsertState(ctx, "", "ephemeral-1", data, 0); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if err := ps.DeleteState(ctx, "", "ephemeral-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	got, version, err := ps.LoadState(ctx, "", "ephemeral-1")
	if err != nil {
		t.Fatalf("load after delete: %v", err)
	}
	if got != nil {
		t.Errorf("data after delete: got %s, want nil", got)
	}
	if version != 0 {
		t.Errorf("version after delete: got %d, want 0", version)
	}
}
