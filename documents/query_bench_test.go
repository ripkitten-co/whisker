//go:build integration

package documents

import (
	"fmt"
	"testing"
)

func BenchmarkQuery_Execute(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_query_exec")
	for i := range 100 {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@test.com", i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Query().Execute(ctx); err != nil {
			b.Fatalf("execute: %v", err)
		}
	}
}

func BenchmarkQuery_Where(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_query_where")
	for i := range 100 {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@test.com", i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Where("name", "=", "User50").Execute(ctx); err != nil {
			b.Fatalf("where: %v", err)
		}
	}
}

func BenchmarkQuery_OrderBy(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_query_order")
	for i := range 100 {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@test.com", i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Query().OrderBy("name", Asc).Execute(ctx); err != nil {
			b.Fatalf("orderby: %v", err)
		}
	}
}

func BenchmarkQuery_Pagination(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_query_page")
	for i := range 100 {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@test.com", i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Query().OrderBy("name", Asc).Limit(10).Offset(50).Execute(ctx); err != nil {
			b.Fatalf("pagination: %v", err)
		}
	}
}

func BenchmarkQuery_Count(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_query_count")
	for i := range 100 {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@test.com", i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Where("name", "=", "User50").Count(ctx); err != nil {
			b.Fatalf("count: %v", err)
		}
	}
}

func BenchmarkQuery_Exists(b *testing.B) {
	store, ctx := setupBench(b)
	users := Collection[benchUser](store, "bench_query_exists")
	for i := range 100 {
		_ = users.Insert(ctx, &benchUser{ID: fmt.Sprintf("u%d", i), Name: fmt.Sprintf("User%d", i), Email: fmt.Sprintf("user%d@test.com", i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := users.Where("name", "=", "User50").Exists(ctx); err != nil {
			b.Fatalf("exists: %v", err)
		}
	}
}
