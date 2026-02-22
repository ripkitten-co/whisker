//go:build integration

package documents

import (
	"context"
	"fmt"
	"testing"

	whisker "github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

type benchUser struct {
	ID      string
	Name    string
	Email   string
	Version int
}

func setupBench(b *testing.B) (*whisker.Store, context.Context) {
	b.Helper()
	connStr := testutil.SetupPostgres(b)
	ctx := context.Background()
	store, err := whisker.New(ctx, connStr)
	if err != nil {
		b.Fatalf("new store: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	return store, ctx
}

func BenchmarkInsert(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_insert")
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		u := &benchUser{ID: fmt.Sprintf("u%d", i), Name: "Alice", Email: "alice@test.com"}
		if err := users.Insert(ctx, u); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkLoad(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_load")
	_ = users.Insert(ctx, &benchUser{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Load(ctx, "u1"); err != nil {
			b.Fatalf("load: %v", err)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_update")
	u := &benchUser{ID: "u1", Name: "Alice", Email: "alice@test.com"}
	_ = users.Insert(ctx, u)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		u.Name = "Bob"
		if err := users.Update(ctx, u); err != nil {
			b.Fatalf("update: %v", err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_delete")
	for i := range b.N {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: "Alice", Email: "alice@test.com"})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := users.Delete(ctx, fmt.Sprintf("u%d", i)); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkCount(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_count")
	for i := range 100 {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: "Alice", Email: "alice@test.com"})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Count(ctx); err != nil {
			b.Fatalf("count: %v", err)
		}
	}
}

func BenchmarkExists(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_exists")
	_ = users.Insert(ctx, &benchUser{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Exists(ctx, "u1"); err != nil {
			b.Fatalf("exists: %v", err)
		}
	}
}
