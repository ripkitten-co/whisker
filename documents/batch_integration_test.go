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

func TestLoadMany_HappyPath(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "load_many_users")

	docs := []*User{
		{ID: "u1", Name: "Alice", Email: "alice@test.com"},
		{ID: "u2", Name: "Bob", Email: "bob@test.com"},
		{ID: "u3", Name: "Charlie", Email: "charlie@test.com"},
	}
	if err := users.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := users.LoadMany(ctx, []string{"u1", "u3"})
	if err != nil {
		t.Fatalf("load many: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d docs, want 2", len(got))
	}

	ids := map[string]bool{}
	for _, doc := range got {
		ids[doc.ID] = true
		if doc.Version != 1 {
			t.Errorf("doc %s: version = %d, want 1", doc.ID, doc.Version)
		}
	}
	if !ids["u1"] || !ids["u3"] {
		t.Errorf("expected u1 and u3, got %v", ids)
	}
}

func TestLoadMany_PartialMissing(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "load_many_partial_users")

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := users.LoadMany(ctx, []string{"u1", "u99"})
	if err == nil {
		t.Fatal("expected error for missing ID")
	}

	var batchErr *documents.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("expected BatchError, got %T: %v", err, err)
	}
	if batchErr.Op != "load" {
		t.Errorf("op = %q, want %q", batchErr.Op, "load")
	}
	if len(batchErr.Errors) != 1 {
		t.Fatalf("errors count = %d, want 1", len(batchErr.Errors))
	}
	if !errors.Is(batchErr.Errors["u99"], whisker.ErrNotFound) {
		t.Errorf("u99 error = %v, want ErrNotFound", batchErr.Errors["u99"])
	}

	if len(got) != 1 {
		t.Fatalf("got %d docs, want 1", len(got))
	}
	if got[0].ID != "u1" {
		t.Errorf("got ID = %q, want u1", got[0].ID)
	}
}

func TestLoadMany_EmptySlice(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "load_many_empty_users")

	got, err := users.LoadMany(ctx, nil)
	if err != nil {
		t.Errorf("nil slice: %v", err)
	}
	if got != nil {
		t.Errorf("nil slice: got %v, want nil", got)
	}

	got, err = users.LoadMany(ctx, []string{})
	if err != nil {
		t.Errorf("empty slice: %v", err)
	}
	if got != nil {
		t.Errorf("empty slice: got %v, want nil", got)
	}
}

func TestDeleteMany_HappyPath(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "delete_many_users")

	docs := []*User{
		{ID: "u1", Name: "Alice", Email: "alice@test.com"},
		{ID: "u2", Name: "Bob", Email: "bob@test.com"},
		{ID: "u3", Name: "Charlie", Email: "charlie@test.com"},
	}
	if err := users.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := users.DeleteMany(ctx, []string{"u1", "u3"}); err != nil {
		t.Fatalf("delete many: %v", err)
	}

	for _, id := range []string{"u1", "u3"} {
		_, err := users.Load(ctx, id)
		if !errors.Is(err, whisker.ErrNotFound) {
			t.Errorf("load %s after delete: got %v, want ErrNotFound", id, err)
		}
	}

	got, err := users.Load(ctx, "u2")
	if err != nil {
		t.Fatalf("load u2: %v", err)
	}
	if got.Name != "Bob" {
		t.Errorf("u2 name = %q, want Bob", got.Name)
	}
}

func TestDeleteMany_PartialMissing(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "delete_many_partial_users")

	if err := users.Insert(ctx, &User{ID: "u1", Name: "Alice"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	err := users.DeleteMany(ctx, []string{"u1", "u99"})
	if err == nil {
		t.Fatal("expected error for missing ID")
	}

	var batchErr *documents.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("expected BatchError, got %T: %v", err, err)
	}
	if batchErr.Op != "delete" {
		t.Errorf("op = %q, want %q", batchErr.Op, "delete")
	}
	if len(batchErr.Errors) != 1 {
		t.Fatalf("errors count = %d, want 1", len(batchErr.Errors))
	}
	if !errors.Is(batchErr.Errors["u99"], whisker.ErrNotFound) {
		t.Errorf("u99 error = %v, want ErrNotFound", batchErr.Errors["u99"])
	}

	// u1 should still be deleted even though u99 was missing
	_, err = users.Load(ctx, "u1")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("load u1 after delete: got %v, want ErrNotFound", err)
	}
}

func TestDeleteMany_EmptySlice(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "delete_many_empty_users")

	if err := users.DeleteMany(ctx, nil); err != nil {
		t.Errorf("nil slice: %v", err)
	}
	if err := users.DeleteMany(ctx, []string{}); err != nil {
		t.Errorf("empty slice: %v", err)
	}
}

func TestUpdateMany_HappyPath(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "update_many_users")

	u1 := &User{ID: "u1", Name: "Alice", Email: "alice@test.com"}
	u2 := &User{ID: "u2", Name: "Bob", Email: "bob@test.com"}
	if err := users.InsertMany(ctx, []*User{u1, u2}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	loaded1, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load u1: %v", err)
	}
	loaded2, err := users.Load(ctx, "u2")
	if err != nil {
		t.Fatalf("load u2: %v", err)
	}

	loaded1.Name = "Alice Updated"
	loaded2.Name = "Bob Updated"

	if err := users.UpdateMany(ctx, []*User{loaded1, loaded2}); err != nil {
		t.Fatalf("update many: %v", err)
	}

	if loaded1.Version != 2 {
		t.Errorf("u1 version = %d, want 2", loaded1.Version)
	}
	if loaded2.Version != 2 {
		t.Errorf("u2 version = %d, want 2", loaded2.Version)
	}

	reloaded1, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("reload u1: %v", err)
	}
	if reloaded1.Name != "Alice Updated" {
		t.Errorf("u1 name = %q, want %q", reloaded1.Name, "Alice Updated")
	}

	reloaded2, err := users.Load(ctx, "u2")
	if err != nil {
		t.Fatalf("reload u2: %v", err)
	}
	if reloaded2.Name != "Bob Updated" {
		t.Errorf("u2 name = %q, want %q", reloaded2.Name, "Bob Updated")
	}
}

func TestUpdateMany_VersionConflict(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "update_many_conflict_users")

	if err := users.InsertMany(ctx, []*User{
		{ID: "u1", Name: "Alice"},
		{ID: "u2", Name: "Bob"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	u1a, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load u1a: %v", err)
	}
	u1b, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load u1b: %v", err)
	}
	u2, err := users.Load(ctx, "u2")
	if err != nil {
		t.Fatalf("load u2: %v", err)
	}

	// u1a wins the race
	u1a.Name = "Alice v2"
	if err := users.Update(ctx, u1a); err != nil {
		t.Fatalf("update u1a: %v", err)
	}

	// u1b is now stale (version=1), u2 is fine (version=1)
	u1b.Name = "Alice stale"
	u2.Name = "Bob Updated"

	err = users.UpdateMany(ctx, []*User{u1b, u2})
	if err == nil {
		t.Fatal("expected error for version conflict")
	}

	var batchErr *documents.BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("expected BatchError, got %T: %v", err, err)
	}
	if batchErr.Op != "update" {
		t.Errorf("op = %q, want %q", batchErr.Op, "update")
	}
	if len(batchErr.Errors) != 1 {
		t.Fatalf("errors count = %d, want 1", len(batchErr.Errors))
	}
	if !errors.Is(batchErr.Errors["u1"], whisker.ErrVersionConflict) {
		t.Errorf("u1 error = %v, want ErrVersionConflict", batchErr.Errors["u1"])
	}
}

func TestUpdateMany_EmptySlice(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "update_many_empty_users")

	if err := users.UpdateMany(ctx, nil); err != nil {
		t.Errorf("nil slice: %v", err)
	}
	if err := users.UpdateMany(ctx, []*User{}); err != nil {
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
