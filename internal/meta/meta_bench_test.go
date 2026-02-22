package meta

import (
	"sync"
	"testing"
)

type benchDoc struct {
	ID      string `whisker:"id"`
	Name    string `whisker:"index"`
	Email   string
	Bio     string
	Version int `whisker:"version"`
}

func BenchmarkAnalyze_Cold(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		cache = sync.Map{} // clear cache each iteration
		Analyze[benchDoc]()
	}
}

func BenchmarkAnalyze_Cached(b *testing.B) {
	Analyze[benchDoc]() // warm cache
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		Analyze[benchDoc]()
	}
}

func BenchmarkExtractID(b *testing.B) {
	doc := benchDoc{ID: "u1", Name: "Alice", Email: "alice@test.com", Version: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = ExtractID(&doc)
	}
}

func BenchmarkExtractVersion(b *testing.B) {
	doc := benchDoc{ID: "u1", Name: "Alice", Email: "alice@test.com", Version: 5}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = ExtractVersion(&doc)
	}
}

func BenchmarkSetVersion(b *testing.B) {
	doc := benchDoc{ID: "u1", Name: "Alice", Email: "alice@test.com", Version: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		SetVersion(&doc, 42)
	}
}

func BenchmarkSetID(b *testing.B) {
	doc := benchDoc{ID: "u1", Name: "Alice", Email: "alice@test.com", Version: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		SetID(&doc, "u999")
	}
}
