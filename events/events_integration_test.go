//go:build integration

package events_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/internal/testutil"
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

func TestEvents_AppendAndReadStream(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	err := es.Append(ctx, "order-1", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"item":"widget"}`)},
		{Type: "OrderPaid", Data: []byte(`{"amount":100}`)},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	got, err := es.ReadStream(ctx, "order-1", 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
	if got[0].Type != "OrderCreated" || got[0].Version != 1 {
		t.Errorf("event[0]: %+v", got[0])
	}
	if got[1].Type != "OrderPaid" || got[1].Version != 2 {
		t.Errorf("event[1]: %+v", got[1])
	}
}

func TestEvents_AppendExpectedVersionConflict(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	es.Append(ctx, "order-1", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{}`)},
	})

	err := es.Append(ctx, "order-1", 0, []events.Event{
		{Type: "Duplicate", Data: []byte(`{}`)},
	})
	if !errors.Is(err, whisker.ErrStreamExists) {
		t.Errorf("got %v, want ErrStreamExists", err)
	}
}

func TestEvents_AppendWrongVersion(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	es.Append(ctx, "order-1", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{}`)},
	})

	err := es.Append(ctx, "order-1", 5, []events.Event{
		{Type: "Late", Data: []byte(`{}`)},
	})
	if !errors.Is(err, whisker.ErrConcurrencyConflict) {
		t.Errorf("got %v, want ErrConcurrencyConflict", err)
	}
}

func TestEvents_ReadStreamFromVersion(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	es := events.New(store)

	es.Append(ctx, "order-1", 0, []events.Event{
		{Type: "A", Data: []byte(`{}`)},
		{Type: "B", Data: []byte(`{}`)},
		{Type: "C", Data: []byte(`{}`)},
	})

	got, err := es.ReadStream(ctx, "order-1", 2)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("got %d events, want 2 (versions 2 and 3)", len(got))
	}
	if got[0].Type != "B" {
		t.Errorf("event[0].Type: got %q, want %q", got[0].Type, "B")
	}
}

func TestEvents_ReadStreamEmpty(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	got, err := events.New(store).ReadStream(ctx, "nonexistent", 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d events, want 0", len(got))
	}
}
