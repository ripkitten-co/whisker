//go:build integration

package whisker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

type Order struct {
	ID      string
	Item    string
	Version int
}

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

func TestSession_CommitDocumentsAndEvents(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	sess, err := store.Session(ctx)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	orders := documents.Collection[Order](sess, "orders")
	err = orders.Insert(ctx, &Order{ID: "o1", Item: "widget"})
	if err != nil {
		t.Fatalf("insert in session: %v", err)
	}

	err = events.New(sess).Append(ctx, "order-o1", 0, []events.Event{
		{Type: "OrderCreated", Data: []byte(`{"item":"widget"}`)},
	})
	if err != nil {
		t.Fatalf("append in session: %v", err)
	}

	err = sess.Commit(ctx)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	order, err := documents.Collection[Order](store, "orders").Load(ctx, "o1")
	if err != nil {
		t.Fatalf("load after commit: %v", err)
	}
	if order.Item != "widget" {
		t.Errorf("item: got %q, want %q", order.Item, "widget")
	}

	evts, err := events.New(store).ReadStream(ctx, "order-o1", 0)
	if err != nil {
		t.Fatalf("read events after commit: %v", err)
	}
	if len(evts) != 1 {
		t.Fatalf("got %d events, want 1", len(evts))
	}
}

func TestSession_RollbackOnError(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	sess, err := store.Session(ctx)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	orders := documents.Collection[Order](sess, "orders")
	orders.Insert(ctx, &Order{ID: "o1", Item: "widget"})

	err = sess.Rollback(ctx)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}

	_, err = documents.Collection[Order](store, "orders").Load(ctx, "o1")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound (data should have been rolled back)", err)
	}
}

func TestSession_CommitEmpty(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	sess, err := store.Session(ctx)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	err = sess.Commit(ctx)
	if err != nil {
		t.Errorf("commit empty session: %v", err)
	}
}
