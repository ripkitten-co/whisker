//go:build integration

package documents_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

func setupConnStr(t *testing.T) string {
	t.Helper()
	return testutil.SetupPostgres(t)
}

func TestInsertMany_HappyPath(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "insert_many_users")

	docs := []*User{
		{ID: "u1", Name: "Alice", Email: "alice@test.com"},
		{ID: "u2", Name: "Bob", Email: "bob@test.com"},
		{ID: "u3", Name: "Charlie", Email: "charlie@test.com"},
	}

	err := users.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("insert many: %v", err)
	}

	for _, doc := range docs {
		if doc.Version != 1 {
			t.Errorf("doc %s: version = %d, want 1", doc.ID, doc.Version)
		}
	}

	for _, id := range []string{"u1", "u2", "u3"} {
		got, err := users.Load(ctx, id)
		if err != nil {
			t.Fatalf("load %s: %v", id, err)
		}
		if got.Version != 1 {
			t.Errorf("loaded %s: version = %d, want 1", id, got.Version)
		}
	}
}

func TestInsertMany_DuplicateID(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "insert_many_dup_users")

	err := users.Insert(ctx, &User{ID: "u1", Name: "Alice"})
	if err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	err = users.InsertMany(ctx, []*User{
		{ID: "u1", Name: "Alice Again"},
		{ID: "u2", Name: "Bob"},
	})
	if err == nil {
		t.Fatal("expected error for duplicate ID")
	}

	var batchErr *documents.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("expected BatchError, got %T: %v", err, err)
	}
	if batchErr.Op != "insert" {
		t.Errorf("op = %q, want %q", batchErr.Op, "insert")
	}
}

func TestInsertMany_EmptySlice(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "insert_many_empty_users")

	if err := users.InsertMany(ctx, nil); err != nil {
		t.Errorf("nil slice: %v", err)
	}
	if err := users.InsertMany(ctx, []*User{}); err != nil {
		t.Errorf("empty slice: %v", err)
	}
}

func TestInsertMany_BatchTooLarge(t *testing.T) {
	connStr := setupConnStr(t)
	store, err := whisker.New(context.Background(), connStr, whisker.WithMaxBatchSize(2))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	ctx := context.Background()
	users := documents.Collection[User](store, "insert_many_limit_users")

	err = users.InsertMany(ctx, []*User{
		{ID: "u1", Name: "Alice"},
		{ID: "u2", Name: "Bob"},
		{ID: "u3", Name: "Charlie"},
	})
	if !errors.Is(err, whisker.ErrBatchTooLarge) {
		t.Errorf("got %v, want ErrBatchTooLarge", err)
	}
}
