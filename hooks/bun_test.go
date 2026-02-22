//go:build integration

package hooks

import (
	"context"
	"testing"

	whisker "github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

func TestBunAdapter_ExecAndQuery(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[poolTestUser](pool, "users")

	adapter := BunAdapter(pool)

	_, err = adapter.ExecContext(ctx,
		"INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
		"u1", "Alice", "alice@test.com",
	)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	rows, err := adapter.QueryContext(ctx,
		"SELECT id, name, email FROM users WHERE id = $1",
		"u1",
	)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row")
	}
}
