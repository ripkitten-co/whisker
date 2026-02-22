//go:build integration

package hooks

import (
	"context"
	"testing"

	whisker "github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/internal/testutil"
	"github.com/uptrace/bun"
)

type BunUser struct {
	bun.BaseModel `bun:"table:users"`
	ID            string `bun:"id,pk"`
	Name          string `bun:"name"`
	Email         string `bun:"email"`
	Version       int    `bun:"version"`
}

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

func TestBun_CreateTableInsertAndSelect(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[BunUser](pool, "users")

	bunDB, adapter := OpenBun(pool)
	defer bunDB.Close()

	// CreateTable through the adapter (rewrites to whisker DDL)
	_, err = bunDB.NewCreateTable().
		Model((*BunUser)(nil)).
		IfNotExists().
		Conn(adapter).
		Exec(ctx)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert via Bun ORM (rewrites inline-valued INSERT to whisker JSONB)
	user := &BunUser{ID: "u1", Name: "Alice", Email: "alice@test.com"}
	_, err = bunDB.NewInsert().Model(user).Conn(adapter).Exec(ctx)
	if err != nil {
		t.Fatalf("bun insert: %v", err)
	}

	// Select via Bun ORM (rewrites qualified SELECT to JSONB extraction)
	var found BunUser
	err = bunDB.NewSelect().Model(&found).Where("id = ?", "u1").Conn(adapter).Scan(ctx)
	if err != nil {
		t.Fatalf("bun select: %v", err)
	}
	if found.Name != "Alice" {
		t.Errorf("name = %q, want Alice", found.Name)
	}
	if found.Email != "alice@test.com" {
		t.Errorf("email = %q, want alice@test.com", found.Email)
	}

	// Verify the data is also readable through whisker's native document API
	users := documents.Collection[poolTestUser](store, "users")
	doc, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("direct load: %v", err)
	}
	if doc.Name != "Alice" {
		t.Errorf("whisker name = %q, want Alice", doc.Name)
	}
	if doc.Email != "alice@test.com" {
		t.Errorf("whisker email = %q, want alice@test.com", doc.Email)
	}
	if doc.Version != 1 {
		t.Errorf("whisker version = %d, want 1", doc.Version)
	}
}
