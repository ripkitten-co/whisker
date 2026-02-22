package documents

import "testing"

type testDoc struct {
	ID      string
	Name    string
	Version int
}

func TestQuery_WhereBuildsSQL(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		op       string
		value    any
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "equality",
			field:    "name",
			op:       "=",
			value:    "Alice",
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>$1 = $2",
			wantArgs: []any{"name", "Alice"},
		},
		{
			name:     "not equal",
			field:    "status",
			op:       "!=",
			value:    "inactive",
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>$1 != $2",
			wantArgs: []any{"status", "inactive"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query[testDoc]{table: "whisker_users"}
			q = q.Where(tt.field, tt.op, tt.value)

			sql, args, err := q.toSQL()
			if err != nil {
				t.Fatalf("toSQL: %v", err)
			}
			if sql != tt.wantSQL {
				t.Errorf("sql:\n got: %s\nwant: %s", sql, tt.wantSQL)
			}
			if len(args) != len(tt.wantArgs) {
				t.Fatalf("args: got %d, want %d", len(args), len(tt.wantArgs))
			}
			for i, a := range args {
				if a != tt.wantArgs[i] {
					t.Errorf("arg[%d]: got %v, want %v", i, a, tt.wantArgs[i])
				}
			}
		})
	}
}

func TestResolveField(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		want    string
		wantErr bool
	}{
		{name: "jsonb field", field: "name", want: "data->>'name'"},
		{name: "table column id", field: "id", want: "id"},
		{name: "table column version", field: "version", want: "version"},
		{name: "table column created_at", field: "created_at", want: "created_at"},
		{name: "table column updated_at", field: "updated_at", want: "updated_at"},
		{name: "raw jsonb expression", field: "data->'addr'->>'city'", want: "data->'addr'->>'city'"},
		{name: "empty field", field: "", wantErr: true},
		{name: "invalid characters", field: "name'; DROP", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveField(tt.field)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQuery_InvalidOperator(t *testing.T) {
	q := &Query[testDoc]{table: "whisker_users"}
	q = q.Where("name", "DROP TABLE", "x")

	_, _, err := q.toSQL()
	if err == nil {
		t.Fatal("expected error for invalid operator")
	}
}
