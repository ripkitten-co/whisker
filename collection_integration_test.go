//go:build integration

package whisker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

type User struct {
	ID      string `whisker:"id" json:"id"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Version int    `whisker:"version" json:"-"`
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
	users := whisker.Collection[User](store, "users")

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
	users := whisker.Collection[User](store, "users")

	_, err := users.Load(ctx, "nonexistent")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestCollection_UpdateWithConcurrency(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := whisker.Collection[User](store, "users")

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
	users := whisker.Collection[User](store, "users")

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
	users := whisker.Collection[User](store, "users")

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
	users := whisker.Collection[User](store, "users")

	err := users.Delete(ctx, "nonexistent")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}
