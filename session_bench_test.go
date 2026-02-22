//go:build integration

package whisker_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/events"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

type sessionBenchDoc struct {
	ID      string
	Name    string
	Version int
}

func setupSessionBench(b *testing.B) (*whisker.Store, context.Context) {
	b.Helper()
	connStr := testutil.SetupPostgres(b)
	ctx := context.Background()
	store, err := whisker.New(ctx, connStr)
	if err != nil {
		b.Fatalf("new store: %v", err)
	}
	b.Cleanup(func() { store.Close() })

	// pre-create table so session benchmarks don't include schema bootstrap
	users := documents.Collection[sessionBenchDoc](store, "bench_session")
	_ = users.Insert(ctx, &sessionBenchDoc{ID: "warmup", Name: "warmup"})
	_ = users.Delete(ctx, "warmup")

	return store, ctx
}

func BenchmarkSession_Commit(b *testing.B) {
	store, ctx := setupSessionBench(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		sess, err := store.Session(ctx)
		if err != nil {
			b.Fatalf("session: %v", err)
		}
		users := documents.Collection[sessionBenchDoc](sess, "bench_session")
		_ = users.Insert(ctx, &sessionBenchDoc{ID: fmt.Sprintf("u%d", i), Name: "Alice"})

		es := events.New(sess)
		_ = es.Append(ctx, fmt.Sprintf("stream-%d", i), 0, []events.Event{
			{Type: "Created", Data: []byte(`{"name":"Alice"}`)},
		})

		if err := sess.Commit(ctx); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

func BenchmarkSession_Rollback(b *testing.B) {
	store, ctx := setupSessionBench(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sess, err := store.Session(ctx)
		if err != nil {
			b.Fatalf("session: %v", err)
		}
		users := documents.Collection[sessionBenchDoc](sess, "bench_session")
		_ = users.Insert(ctx, &sessionBenchDoc{ID: "rollback-test", Name: "Alice"})
		_ = sess.Rollback(ctx)
	}
}
