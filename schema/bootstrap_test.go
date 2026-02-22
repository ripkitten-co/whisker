package schema

import "testing"

func TestCollectionDDL(t *testing.T) {
	ddl := collectionDDL("users")
	want := `CREATE TABLE IF NOT EXISTS whisker_users (
	id TEXT PRIMARY KEY,
	data JSONB NOT NULL,
	version INTEGER NOT NULL DEFAULT 1,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`
	if ddl != want {
		t.Errorf("got:\n%s\nwant:\n%s", ddl, want)
	}
}

func TestEventsDDL(t *testing.T) {
	ddl := eventsDDL()
	want := `CREATE TABLE IF NOT EXISTS whisker_events (
	stream_id TEXT NOT NULL,
	version INTEGER NOT NULL,
	type TEXT NOT NULL,
	data JSONB NOT NULL,
	metadata JSONB,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	global_position BIGINT GENERATED ALWAYS AS IDENTITY,
	PRIMARY KEY (stream_id, version)
)`
	if ddl != want {
		t.Errorf("got:\n%s\nwant:\n%s", ddl, want)
	}
}

func TestProjectionCheckpointsDDL(t *testing.T) {
	ddl := projectionCheckpointsDDL()
	want := `CREATE TABLE IF NOT EXISTS whisker_projection_checkpoints (
	projection_name TEXT PRIMARY KEY,
	last_position BIGINT NOT NULL DEFAULT 0,
	status TEXT NOT NULL DEFAULT 'running',
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`
	if ddl != want {
		t.Errorf("got:\n%s\nwant:\n%s", ddl, want)
	}
}

func TestValidateCollectionName(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{"users", true},
		{"order_items", true},
		{"Users123", true},
		{"", false},
		{"drop table;--", false},
		{"has space", false},
		{"has-dash", false},
	}
	for _, tt := range tests {
		err := ValidateCollectionName(tt.name)
		if (err == nil) != tt.valid {
			t.Errorf("ValidateCollectionName(%q): got err=%v, wantValid=%v", tt.name, err, tt.valid)
		}
	}
}

func TestBootstrap_TracksCreated(t *testing.T) {
	b := New()
	if b.IsCreated("whisker_users") {
		t.Error("should not be created yet")
	}
	b.MarkCreated("whisker_users")
	if !b.IsCreated("whisker_users") {
		t.Error("should be created")
	}
}

func TestBootstrap_TracksIndexes(t *testing.T) {
	b := New()
	name := "idx_whisker_users_name"
	if b.IsIndexCreated(name) {
		t.Error("should not be created yet")
	}
	b.MarkIndexCreated(name)
	if !b.IsIndexCreated(name) {
		t.Error("should be created")
	}
}
