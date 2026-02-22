//go:build integration

package hooks

import (
	"context"
	"testing"

	whisker "github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

func TestEntDriver_ExecAndQuery(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[poolTestUser](pool, "users")

	driver := EntDriver(pool)

	_, err = driver.ExecContext(ctx,
		"INSERT INTO users (id, name, email) VALUES ($1, $2, $3)",
		"u1", "Alice", "alice@test.com",
	)
	if err != nil {
		t.Fatalf("exec: %v", err)
	}

	rows, err := driver.QueryContext(ctx,
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

// TestEntDriver_QuotedIdentifiers verifies that the adapter handles SQL in
// the exact format Ent's codegen produces for PostgreSQL: double-quoted
// table and column names, qualified column references, $N placeholders.
//
// A full Ent integration test requires running `go generate` with an ent
// schema, which adds a codegen step that isn't practical for this test suite.
// Instead, we feed Ent-format SQL directly through the adapter to prove the
// rewriters handle Ent's quoting conventions.
func TestEntDriver_QuotedIdentifiers(t *testing.T) {
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()

	store, err := whisker.New(ctx, connStr)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	pool := NewPool(store)
	Register[poolTestUser](pool, "users")

	driver := EntDriver(pool)

	// Ent-style CREATE TABLE with quoted identifiers
	_, err = driver.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS "users" ("id" TEXT NOT NULL, "name" TEXT NOT NULL, "email" TEXT NOT NULL, "version" INTEGER NOT NULL DEFAULT 0, PRIMARY KEY ("id"))`,
	)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Ent-style INSERT with quoted identifiers and $N placeholders
	_, err = driver.ExecContext(ctx,
		`INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`,
		"u1", "Alice", "alice@test.com",
	)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Ent-style SELECT with qualified quoted columns
	rows, err := driver.QueryContext(ctx,
		`SELECT "users"."id", "users"."name", "users"."email" FROM "users" WHERE "users"."id" = $1`,
		"u1",
	)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	// Verify through whisker's native API
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
