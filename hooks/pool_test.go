//go:build integration

package hooks

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

type poolTestUser struct {
	ID      string
	Name    string
	Email   string
	Version int
}

func TestPool_InsertAndQuery(t *testing.T) {
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

	rows, err := pool.Query(ctx,
		"SELECT id, name, email, version FROM users WHERE id = $1",
		"u1",
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected one row")
	}

	var id, name, email string
	var version int
	if err := rows.Scan(&id, &name, &email, &version); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if id != "u1" || name != "Alice" || email != "alice@test.com" {
		t.Errorf("got (%s, %s, %s)", id, name, email)
	}
	if version != 1 {
		t.Errorf("version = %d, want 1", version)
	}
}

func TestPool_Passthrough(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)

	_, err = pool.Exec(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("passthrough: %v", err)
	}
}
