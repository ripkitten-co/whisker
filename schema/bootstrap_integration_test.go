//go:build integration

package schema

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

func setupSchemaTest(t *testing.T) (pg.Executor, context.Context) {
	t.Helper()
	connStr := testutil.SetupPostgres(t)
	ctx := context.Background()
	pool, err := pg.NewPool(ctx, connStr)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool, ctx
}

func TestEnsureProjectionCheckpoints(t *testing.T) {
	exec, ctx := setupSchemaTest(t)
	b := New()

	if err := b.EnsureProjectionCheckpoints(ctx, exec); err != nil {
		t.Fatalf("first call: %v", err)
	}

	if !b.IsCreated("whisker_projection_checkpoints") {
		t.Fatal("table should be cached after creation")
	}

	// second call hits the cache path
	if err := b.EnsureProjectionCheckpoints(ctx, exec); err != nil {
		t.Fatalf("cached call: %v", err)
	}

	// verify table actually exists by inserting a row
	_, err := exec.Exec(ctx,
		`INSERT INTO whisker_projection_checkpoints (projection_name, last_position, status)
		 VALUES ($1, $2, $3)`,
		"test_projection", 42, "running",
	)
	if err != nil {
		t.Fatalf("insert checkpoint row: %v", err)
	}

	var pos int64
	row := exec.QueryRow(ctx,
		`SELECT last_position FROM whisker_projection_checkpoints WHERE projection_name = $1`,
		"test_projection",
	)
	if err := row.Scan(&pos); err != nil {
		t.Fatalf("read checkpoint row: %v", err)
	}
	if pos != 42 {
		t.Errorf("last_position: got %d, want 42", pos)
	}
}
