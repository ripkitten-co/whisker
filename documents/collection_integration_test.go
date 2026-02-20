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

type User struct {
	ID      string
	Name    string
	Email   string
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

func TestCollection_InsertAndLoad(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.ID != "u1" || got.Name != "Alice" || got.Email != "alice@test.com" {
		t.Errorf("got %+v", got)
	}
	if got.Version != 1 {
		t.Errorf("version: got %d, want 1", got.Version)
	}
}

func TestCollection_LoadNotFound(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	_, err := users.Load(ctx, "nonexistent")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestCollection_UpdateWithConcurrency(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice"})
	user, _ := users.Load(ctx, "u1")

	user.Name = "Bob"
	err := users.Update(ctx, user)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if user.Version != 2 {
		t.Errorf("version after update: got %d, want 2", user.Version)
	}

	reloaded, _ := users.Load(ctx, "u1")
	if reloaded.Name != "Bob" {
		t.Errorf("name: got %q, want %q", reloaded.Name, "Bob")
	}
}

func TestCollection_UpdateConcurrencyConflict(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice"})
	user1, _ := users.Load(ctx, "u1")
	user2, _ := users.Load(ctx, "u1")

	user1.Name = "Bob"
	users.Update(ctx, user1)

	user2.Name = "Charlie"
	err := users.Update(ctx, user2)
	if !errors.Is(err, whisker.ErrConcurrencyConflict) {
		t.Errorf("got %v, want ErrConcurrencyConflict", err)
	}
}

func TestCollection_Delete(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice"})
	err := users.Delete(ctx, "u1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = users.Load(ctx, "u1")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestCollection_DeleteNotFound(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	err := users.Delete(ctx, "nonexistent")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestCollection_WhereQuery(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	users.Insert(ctx, &User{ID: "u2", Name: "Bob", Email: "bob@test.com"})
	users.Insert(ctx, &User{ID: "u3", Name: "Alice", Email: "alice2@test.com"})

	results, err := users.Where("name", "=", "Alice").Execute(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
}

func TestCollection_WhereQueryNoResults(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	results, err := users.Where("name", "=", "Nobody").Execute(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want 0", len(results))
	}
}

type TagOverrideUser struct {
	Key     string `whisker:"id"`
	Name    string `json:"display_name"`
	Version int
}

func TestCollection_TagOverrides(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[TagOverrideUser](store, "tag_users")

	err := users.Insert(ctx, &TagOverrideUser{Key: "u1", Name: "Alice"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Key != "u1" || got.Name != "Alice" {
		t.Errorf("got %+v", got)
	}
	if got.Version != 1 {
		t.Errorf("version: got %d, want 1", got.Version)
	}

	results, err := users.Where("display_name", "=", "Alice").Execute(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("got %d results, want 1", len(results))
	}
}
