//go:build integration

package documents_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/documents"
	"github.com/ripkitten-co/whisker/internal/testutil"
)

type User struct {
	ID      string
	Name    string
	Email   string
	Version int
}

func setupStore(t *testing.T) *whisker.Store {
	t.Helper()
	connStr := testutil.SetupPostgres(t)
	store, err := whisker.New(context.Background(), connStr)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestCollection_InsertAndLoad(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	err := users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if got.ID != "u1" || got.Name != "Alice" || got.Email != "alice@test.com" {
		t.Errorf("got %+v", got)
	}
	if got.Version != 1 {
		t.Errorf("version: got %d, want 1", got.Version)
	}
}

func TestCollection_LoadNotFound(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	_, err := users.Load(ctx, "nonexistent")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestCollection_UpdateWithConcurrency(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice"})
	user, _ := users.Load(ctx, "u1")

	user.Name = "Bob"
	err := users.Update(ctx, user)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if user.Version != 2 {
		t.Errorf("version after update: got %d, want 2", user.Version)
	}

	reloaded, _ := users.Load(ctx, "u1")
	if reloaded.Name != "Bob" {
		t.Errorf("name: got %q, want %q", reloaded.Name, "Bob")
	}
}

func TestCollection_UpdateConcurrencyConflict(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice"})
	user1, _ := users.Load(ctx, "u1")
	user2, _ := users.Load(ctx, "u1")

	user1.Name = "Bob"
	users.Update(ctx, user1)

	user2.Name = "Charlie"
	err := users.Update(ctx, user2)
	if !errors.Is(err, whisker.ErrConcurrencyConflict) {
		t.Errorf("got %v, want ErrConcurrencyConflict", err)
	}
}

func TestCollection_Delete(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice"})
	err := users.Delete(ctx, "u1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = users.Load(ctx, "u1")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestCollection_DeleteNotFound(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	err := users.Delete(ctx, "nonexistent")
	if !errors.Is(err, whisker.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestCollection_WhereQuery(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	users.Insert(ctx, &User{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	users.Insert(ctx, &User{ID: "u2", Name: "Bob", Email: "bob@test.com"})
	users.Insert(ctx, &User{ID: "u3", Name: "Alice", Email: "alice2@test.com"})

	results, err := users.Where("name", "=", "Alice").Execute(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
}

func TestCollection_WhereQueryNoResults(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[User](store, "users")

	results, err := users.Where("name", "=", "Nobody").Execute(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want 0", len(results))
	}
}

type IndexedUser struct {
	ID      string `whisker:"id"`
	Name    string `whisker:"index"`
	Email   string `whisker:"index"`
	Version int    `whisker:"version"`
}

type GINUser struct {
	ID      string `whisker:"id"`
	Name    string
	Tags    []string `whisker:"index,gin"`
	Version int      `whisker:"version"`
}

type TagOverrideUser struct {
	Key     string `whisker:"id"`
	Name    string `json:"display_name"`
	Version int
}

func TestCollection_TagOverrides(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[TagOverrideUser](store, "tag_users")

	err := users.Insert(ctx, &TagOverrideUser{Key: "u1", Name: "Alice"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := users.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Key != "u1" || got.Name != "Alice" {
		t.Errorf("got %+v", got)
	}
	if got.Version != 1 {
		t.Errorf("version: got %d, want 1", got.Version)
	}

	results, err := users.Where("display_name", "=", "Alice").Execute(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("got %d results, want 1", len(results))
	}
}

func TestCollection_BtreeIndexesCreated(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[IndexedUser](store, "idx_users")

	err := users.Insert(ctx, &IndexedUser{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var count int
	err = store.DBExecutor().QueryRow(ctx,
		"SELECT count(*) FROM pg_indexes WHERE tablename = 'whisker_idx_users' AND indexname LIKE 'idx_whisker_idx_users_%'",
	).Scan(&count)
	if err != nil {
		t.Fatalf("query pg_indexes: %v", err)
	}
	if count != 2 {
		t.Errorf("index count = %d, want 2 (name + email)", count)
	}
}

func TestCollection_GINIndexCreated(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	users := documents.Collection[GINUser](store, "gin_users")

	err := users.Insert(ctx, &GINUser{ID: "u1", Name: "Alice", Tags: []string{"admin"}})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var count int
	err = store.DBExecutor().QueryRow(ctx,
		"SELECT count(*) FROM pg_indexes WHERE tablename = 'whisker_gin_users' AND indexname = 'idx_whisker_gin_users_data_gin'",
	).Scan(&count)
	if err != nil {
		t.Fatalf("query pg_indexes: %v", err)
	}
	if count != 1 {
		t.Errorf("GIN index count = %d, want 1", count)
	}
}

func TestCollection_SessionSkipsIndexCreation(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	sess, err := store.Session(ctx)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer sess.Close(ctx)

	users := documents.Collection[IndexedUser](sess, "sess_idx_users")
	err = users.Insert(ctx, &IndexedUser{ID: "u1", Name: "Alice", Email: "alice@test.com"})
	if err != nil {
		t.Fatalf("insert in session: %v", err)
	}
	sess.Commit(ctx)

	var count int
	err = store.DBExecutor().QueryRow(ctx,
		"SELECT count(*) FROM pg_indexes WHERE tablename = 'whisker_sess_idx_users' AND indexname LIKE 'idx_whisker_%'",
	).Scan(&count)
	if err != nil {
		t.Fatalf("query pg_indexes: %v", err)
	}
	if count != 0 {
		t.Errorf("index count = %d, want 0 (should be skipped in transaction)", count)
	}

	users2 := documents.Collection[IndexedUser](store, "sess_idx_users")
	_, err = users2.Load(ctx, "u1")
	if err != nil {
		t.Fatalf("load outside session: %v", err)
	}

	err = store.DBExecutor().QueryRow(ctx,
		"SELECT count(*) FROM pg_indexes WHERE tablename = 'whisker_sess_idx_users' AND indexname LIKE 'idx_whisker_%'",
	).Scan(&count)
	if err != nil {
		t.Fatalf("query pg_indexes after non-tx use: %v", err)
	}
	if count != 2 {
		t.Errorf("index count = %d, want 2 (created outside transaction)", count)
	}
}
