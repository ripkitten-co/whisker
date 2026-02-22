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

func BenchmarkInsertMany(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			store, ctx := setupBench(b)
			users := Collection[benchUser](store, fmt.Sprintf("bench_insert_many_%d", size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				docs := make([]*benchUser, size)
				for j := range size {
					docs[j] = &benchUser{
						ID:    fmt.Sprintf("u%d_%d", i, j),
						Name:  "Alice",
						Email: "alice@test.com",
					}
				}
				if err := users.InsertMany(ctx, docs); err != nil {
					b.Fatalf("insert many: %v", err)
				}
			}
		})
	}
}

func BenchmarkLoadMany(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			store, ctx := setupBench(b)
			users := Collection[benchUser](store, fmt.Sprintf("bench_load_many_%d", size))
			ids := make([]string, size)
			docs := make([]*benchUser, size)
			for j := range size {
				ids[j] = fmt.Sprintf("u%d", j)
				docs[j] = &benchUser{
					ID:    ids[j],
					Name:  "Alice",
					Email: "alice@test.com",
				}
			}
			if err := users.InsertMany(ctx, docs); err != nil {
				b.Fatalf("setup insert many: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := users.LoadMany(ctx, ids); err != nil {
					b.Fatalf("load many: %v", err)
				}
			}
		})
	}
}

func BenchmarkInsertMany_VsLoop(b *testing.B) {
	const size = 50

	b.Run("batch", func(b *testing.B) {
		store, ctx := setupBench(b)
		users := Collection[benchUser](store, "bench_insert_many_vs_batch")
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			docs := make([]*benchUser, size)
			for j := range size {
				docs[j] = &benchUser{
					ID:    fmt.Sprintf("u%d_%d", i, j),
					Name:  "Alice",
					Email: "alice@test.com",
				}
			}
			if err := users.InsertMany(ctx, docs); err != nil {
				b.Fatalf("insert many: %v", err)
			}
		}
	})

	b.Run("loop", func(b *testing.B) {
		store, ctx := setupBench(b)
		users := Collection[benchUser](store, "bench_insert_many_vs_loop")
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			for j := range size {
				u := &benchUser{
					ID:    fmt.Sprintf("u%d_%d", i, j),
					Name:  "Alice",
					Email: "alice@test.com",
				}
				if err := users.Insert(ctx, u); err != nil {
					b.Fatalf("insert: %v", err)
				}
			}
		}
	})
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
