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
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>'name' = $1",
			wantArgs: []any{"Alice"},
		},
		{
			name:     "not equal",
			field:    "status",
			op:       "!=",
			value:    "inactive",
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>'status' != $1",
			wantArgs: []any{"inactive"},
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

func TestQuery_OrderBySQL(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(q *Query[testDoc]) *Query[testDoc]
		wantSQL  string
		wantArgs []any
	}{
		{
			name:    "single jsonb field asc",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q.OrderBy("name", Asc) },
			wantSQL: "SELECT id, data, version FROM whisker_users ORDER BY data->>'name' ASC",
		},
		{
			name:    "table column desc",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q.OrderBy("created_at", Desc) },
			wantSQL: "SELECT id, data, version FROM whisker_users ORDER BY created_at DESC",
		},
		{
			name: "multiple clauses",
			setup: func(q *Query[testDoc]) *Query[testDoc] {
				return q.OrderBy("name", Asc).OrderBy("created_at", Desc)
			},
			wantSQL: "SELECT id, data, version FROM whisker_users ORDER BY data->>'name' ASC, created_at DESC",
		},
		{
			name:    "raw expression",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q.OrderBy("data->'addr'->>'city'", Asc) },
			wantSQL: "SELECT id, data, version FROM whisker_users ORDER BY data->'addr'->>'city' ASC",
		},
		{
			name: "where plus order",
			setup: func(q *Query[testDoc]) *Query[testDoc] {
				return q.Where("name", "=", "Alice").OrderBy("name", Asc)
			},
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>'name' = $1 ORDER BY data->>'name' ASC",
			wantArgs: []any{"Alice"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query[testDoc]{table: "whisker_users"}
			q = tt.setup(q)
			gotSQL, gotArgs, err := q.toSQL()
			if err != nil {
				t.Fatalf("toSQL: %v", err)
			}
			if gotSQL != tt.wantSQL {
				t.Errorf("sql:\n got: %s\nwant: %s", gotSQL, tt.wantSQL)
			}
			if len(tt.wantArgs) == 0 && len(gotArgs) == 0 {
				return
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args: got %d, want %d", len(gotArgs), len(tt.wantArgs))
			}
			for i, a := range gotArgs {
				if a != tt.wantArgs[i] {
					t.Errorf("arg[%d]: got %v, want %v", i, a, tt.wantArgs[i])
				}
			}
		})
	}
}

func TestQuery_LimitOffsetSQL(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(q *Query[testDoc]) *Query[testDoc]
		wantSQL  string
		wantArgs []any
	}{
		{
			name:    "limit only",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q.Limit(10) },
			wantSQL: "SELECT id, data, version FROM whisker_users LIMIT 10",
		},
		{
			name:    "offset only",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q.Offset(20) },
			wantSQL: "SELECT id, data, version FROM whisker_users OFFSET 20",
		},
		{
			name:    "limit and offset",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q.Limit(10).Offset(20) },
			wantSQL: "SELECT id, data, version FROM whisker_users LIMIT 10 OFFSET 20",
		},
		{
			name:    "limit zero ignored",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q.Limit(0) },
			wantSQL: "SELECT id, data, version FROM whisker_users",
		},
		{
			name: "full chain",
			setup: func(q *Query[testDoc]) *Query[testDoc] {
				return q.Where("name", "=", "Alice").OrderBy("name", Asc).Limit(10).Offset(5)
			},
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>'name' = $1 ORDER BY data->>'name' ASC LIMIT 10 OFFSET 5",
			wantArgs: []any{"Alice"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query[testDoc]{table: "whisker_users"}
			q = tt.setup(q)
			gotSQL, gotArgs, err := q.toSQL()
			if err != nil {
				t.Fatalf("toSQL: %v", err)
			}
			if gotSQL != tt.wantSQL {
				t.Errorf("sql:\n got: %s\nwant: %s", gotSQL, tt.wantSQL)
			}
			if len(tt.wantArgs) == 0 && len(gotArgs) == 0 {
				return
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args: got %d, want %d", len(gotArgs), len(tt.wantArgs))
			}
			for i, a := range gotArgs {
				if a != tt.wantArgs[i] {
					t.Errorf("arg[%d]: got %v, want %v", i, a, tt.wantArgs[i])
				}
			}
		})
	}
}

func TestQuery_AfterSQL(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(q *Query[testDoc]) *Query[testDoc]
		wantSQL  string
		wantArgs []any
		wantErr  bool
	}{
		{
			name: "after with asc",
			setup: func(q *Query[testDoc]) *Query[testDoc] {
				return q.OrderBy("name", Asc).Limit(10).After("Charlie")
			},
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>'name' > $1 ORDER BY data->>'name' ASC LIMIT 10",
			wantArgs: []any{"Charlie"},
		},
		{
			name: "after with desc",
			setup: func(q *Query[testDoc]) *Query[testDoc] {
				return q.OrderBy("created_at", Desc).Limit(10).After("2024-01-15")
			},
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE created_at < $1 ORDER BY created_at DESC LIMIT 10",
			wantArgs: []any{"2024-01-15"},
		},
		{
			name: "after with where and order",
			setup: func(q *Query[testDoc]) *Query[testDoc] {
				return q.Where("name", "!=", "deleted").OrderBy("name", Asc).After("Bob")
			},
			wantSQL:  "SELECT id, data, version FROM whisker_users WHERE data->>'name' != $1 AND data->>'name' > $2 ORDER BY data->>'name' ASC",
			wantArgs: []any{"deleted", "Bob"},
		},
		{
			name: "after without order by fails",
			setup: func(q *Query[testDoc]) *Query[testDoc] {
				return q.After("some-value")
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query[testDoc]{table: "whisker_users"}
			q = tt.setup(q)
			gotSQL, gotArgs, err := q.toSQL()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("toSQL: %v", err)
			}
			if gotSQL != tt.wantSQL {
				t.Errorf("sql:\n got: %s\nwant: %s", gotSQL, tt.wantSQL)
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args: got %v, want %v", gotArgs, tt.wantArgs)
			}
			for i, a := range gotArgs {
				if a != tt.wantArgs[i] {
					t.Errorf("arg[%d]: got %v, want %v", i, a, tt.wantArgs[i])
				}
			}
		})
	}
}

func TestQuery_CountSQL(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(q *Query[testDoc]) *Query[testDoc]
		wantSQL  string
		wantArgs []any
	}{
		{
			name:    "count all",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q },
			wantSQL: "SELECT COUNT(*) FROM whisker_users",
		},
		{
			name:     "count with where",
			setup:    func(q *Query[testDoc]) *Query[testDoc] { return q.Where("name", "=", "Alice") },
			wantSQL:  "SELECT COUNT(*) FROM whisker_users WHERE data->>'name' = $1",
			wantArgs: []any{"Alice"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query[testDoc]{table: "whisker_users"}
			q = tt.setup(q)
			gotSQL, gotArgs, err := q.toCountSQL()
			if err != nil {
				t.Fatalf("toCountSQL: %v", err)
			}
			if gotSQL != tt.wantSQL {
				t.Errorf("sql:\n got: %s\nwant: %s", gotSQL, tt.wantSQL)
			}
			if len(tt.wantArgs) == 0 && len(gotArgs) == 0 {
				return
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args: got %d, want %d", len(gotArgs), len(tt.wantArgs))
			}
			for i, a := range gotArgs {
				if a != tt.wantArgs[i] {
					t.Errorf("arg[%d]: got %v, want %v", i, a, tt.wantArgs[i])
				}
			}
		})
	}
}

func TestQuery_ExistsSQL(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(q *Query[testDoc]) *Query[testDoc]
		wantSQL  string
		wantArgs []any
	}{
		{
			name:    "exists all",
			setup:   func(q *Query[testDoc]) *Query[testDoc] { return q },
			wantSQL: "SELECT EXISTS(SELECT 1 FROM whisker_users)",
		},
		{
			name:     "exists with where",
			setup:    func(q *Query[testDoc]) *Query[testDoc] { return q.Where("email", "=", "alice@test.com") },
			wantSQL:  "SELECT EXISTS(SELECT 1 FROM whisker_users WHERE data->>'email' = $1)",
			wantArgs: []any{"alice@test.com"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query[testDoc]{table: "whisker_users"}
			q = tt.setup(q)
			gotSQL, gotArgs, err := q.toExistsSQL()
			if err != nil {
				t.Fatalf("toExistsSQL: %v", err)
			}
			if gotSQL != tt.wantSQL {
				t.Errorf("sql:\n got: %s\nwant: %s", gotSQL, tt.wantSQL)
			}
			if len(tt.wantArgs) == 0 && len(gotArgs) == 0 {
				return
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args: got %d, want %d", len(gotArgs), len(tt.wantArgs))
			}
			for i, a := range gotArgs {
				if a != tt.wantArgs[i] {
					t.Errorf("arg[%d]: got %v, want %v", i, a, tt.wantArgs[i])
				}
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
