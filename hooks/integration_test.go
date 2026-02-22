//go:build integration

package hooks

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

func TestMiddleware_RoundTrip(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[poolTestUser](pool, "users")

	_, err = pool.Exec(ctx,
		"INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
		"u1", "Alice", "alice@test.com",
	)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	users := documents.Collection[poolTestUser](store, "users")
	doc, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("direct load: %v", err)
	}
	if doc.Name != "Alice" {
		t.Errorf("name = %q, want Alice", doc.Name)
	}
	if doc.Email != "alice@test.com" {
		t.Errorf("email = %q, want alice@test.com", doc.Email)
	}
	if doc.Version != 1 {
		t.Errorf("version = %d, want 1", doc.Version)
	}
}

func TestMiddleware_UpdateAndReload(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[poolTestUser](pool, "users")

	_, err = pool.Exec(ctx,
		"INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
		"u1", "Alice", "alice@test.com",
	)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = pool.Exec(ctx,
		"UPDATE users SET name = $1 WHERE id = $2",
		"Bob", "u1",
	)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	users := documents.Collection[poolTestUser](store, "users")
	doc, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("direct load: %v", err)
	}
	if doc.Name != "Bob" {
		t.Errorf("name = %q, want Bob", doc.Name)
	}
	if doc.Version != 2 {
		t.Errorf("version = %d, want 2", doc.Version)
	}
}

func TestMiddleware_DeleteAndVerify(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[poolTestUser](pool, "users")

	_, err = pool.Exec(ctx,
		"INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
		"u1", "Alice", "alice@test.com",
	)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = pool.Exec(ctx,
		"DELETE FROM users WHERE id = $1",
		"u1",
	)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	users := documents.Collection[poolTestUser](store, "users")
	_, err = users.Load(ctx, "u1")
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
}
