//go:build integration

package events

import (
	"context"
	"fmt"
	"testing"

	whisker "github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

func setupEventBench(b *testing.B) (*whisker.Store, context.Context) {
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

func BenchmarkAppend(b *testing.B) {
	store, ctx := setupEventBench(b)
	es := New(store)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		streamID := fmt.Sprintf("stream-%d", i)
		err := es.Append(ctx, streamID, 0, []Event{
			{Type: "UserCreated", Data: []byte(`{"name":"Alice"}`)},
		})
		if err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func BenchmarkAppendBatch(b *testing.B) {
	store, ctx := setupEventBench(b)
	es := New(store)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		streamID := fmt.Sprintf("batch-%d", i)
		evts := make([]Event, 10)
		for j := range 10 {
			evts[j] = Event{
				Type: "ItemAdded",
				Data: []byte(fmt.Sprintf(`{"item":%d}`, j)),
			}
		}
		if err := es.Append(ctx, streamID, 0, evts); err != nil {
			b.Fatalf("append batch: %v", err)
		}
	}
}

func BenchmarkReadStream(b *testing.B) {
	store, ctx := setupEventBench(b)
	es := New(store)
	evts := make([]Event, 20)
	for j := range 20 {
		evts[j] = Event{Type: "ItemAdded", Data: []byte(fmt.Sprintf(`{"item":%d}`, j))}
	}
	_ = es.Append(ctx, "read-bench", 0, evts)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := es.ReadStream(ctx, "read-bench", 0); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}
