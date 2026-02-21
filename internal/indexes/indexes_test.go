package indexes

import (
	"testing"

	"github.com/ripkitten-co/whisker/internal/meta"
)

func TestBtreeDDL(t *testing.T) {
	got := btreeDDL("users", "name")
	want := `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_users_name ON whisker_users ((data->>'name'))`
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestGINDDL(t *testing.T) {
	got := ginDDL("users")
	want := `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_users_data_gin ON whisker_users USING GIN (data)`
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestIndexDDLs(t *testing.T) {
	indexes := []meta.IndexMeta{
		{FieldJSONKey: "name", Type: meta.IndexBtree},
		{FieldJSONKey: "email", Type: meta.IndexBtree},
		{Type: meta.IndexGIN},
	}

	ddls := IndexDDLs("users", indexes)
	if len(ddls) != 3 {
		t.Fatalf("len(ddls) = %d, want 3", len(ddls))
	}

	wantDDLs := []string{
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_users_name ON whisker_users ((data->>'name'))`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_users_email ON whisker_users ((data->>'email'))`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_whisker_users_data_gin ON whisker_users USING GIN (data)`,
	}
	for i, want := range wantDDLs {
		if ddls[i] != want {
			t.Errorf("ddls[%d]:\n got: %s\nwant: %s", i, ddls[i], want)
		}
	}
}

func TestIndexDDLs_Empty(t *testing.T) {
	ddls := IndexDDLs("users", nil)
	if len(ddls) != 0 {
		t.Errorf("len(ddls) = %d, want 0", len(ddls))
	}
}

func TestIndexName_Btree(t *testing.T) {
	got := IndexName("users", meta.IndexMeta{FieldJSONKey: "email", Type: meta.IndexBtree})
	if got != "idx_whisker_users_email" {
		t.Errorf("got %q", got)
	}
}

func TestIndexName_GIN(t *testing.T) {
	got := IndexName("users", meta.IndexMeta{Type: meta.IndexGIN})
	if got != "idx_whisker_users_data_gin" {
		t.Errorf("got %q", got)
	}
}
