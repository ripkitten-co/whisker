//go:build integration

package schema

import (
	"context"
	"testing"

	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

func setupSchemaBench(b *testing.B) (pg.Executor, context.Context) {
	b.Helper()
	connStr := testutil.SetupPostgres(b)
	ctx := context.Background()
	pool, err := pg.NewPool(ctx, connStr)
	if err != nil {
		b.Fatalf("new pool: %v", err)
	}
	b.Cleanup(func() { pool.Close() })
	return pool, ctx
}

func BenchmarkEnsureCollection_Cold(b *testing.B) {
	exec, ctx := setupSchemaBench(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		bs := New()
		if err := bs.EnsureCollection(ctx, exec, "bench_cold"); err != nil {
			b.Fatalf("ensure: %v", err)
		}
		_ = i
	}
}

func BenchmarkEnsureCollection_Cached(b *testing.B) {
	exec, ctx := setupSchemaBench(b)
	bs := New()
	_ = bs.EnsureCollection(ctx, exec, "bench_cached")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := bs.EnsureCollection(ctx, exec, "bench_cached"); err != nil {
			b.Fatalf("ensure: %v", err)
		}
	}
}

func BenchmarkEnsureEvents_Cold(b *testing.B) {
	exec, ctx := setupSchemaBench(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		bs := New()
		if err := bs.EnsureEvents(ctx, exec); err != nil {
			b.Fatalf("ensure events: %v", err)
		}
		_ = i
	}
}

func BenchmarkEnsureEvents_Cached(b *testing.B) {
	exec, ctx := setupSchemaBench(b)
	bs := New()
	_ = bs.EnsureEvents(ctx, exec)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := bs.EnsureEvents(ctx, exec); err != nil {
			b.Fatalf("ensure events: %v", err)
		}
	}
}
