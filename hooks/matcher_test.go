package hooks

import "testing"

func TestMatcher_Insert(t *testing.T) {
	tests := []struct {
		sql   string
		table string
		op    sqlOp
	}{
		{"INSERT INTO users (id, name) VALUES ($1, $2)", "users", opInsert},
		{"INSERT INTO \"users\" (id, name) VALUES ($1, $2)", "users", opInsert},
		{"insert into users (id) values ($1)", "users", opInsert},
	}
	for _, tt := range tests {
		table, op, ok := parseSQL(tt.sql)
		if !ok {
			t.Errorf("parseSQL(%q) not matched", tt.sql)
			continue
		}
		if table != tt.table {
			t.Errorf("table = %q, want %q", table, tt.table)
		}
		if op != tt.op {
			t.Errorf("op = %v, want %v", op, tt.op)
		}
	}
}

func TestMatcher_Select(t *testing.T) {
	tests := []struct {
		sql   string
		table string
	}{
		{"SELECT id, name FROM users WHERE id = $1", "users"},
		{"SELECT * FROM users", "users"},
		{"SELECT count(*) FROM users WHERE name = $1", "users"},
		{"select id from users where id = $1", "users"},
	}
	for _, tt := range tests {
		table, op, ok := parseSQL(tt.sql)
		if !ok {
			t.Errorf("parseSQL(%q) not matched", tt.sql)
			continue
		}
		if table != tt.table {
			t.Errorf("table = %q, want %q", table, tt.table)
		}
		if op != opSelect {
			t.Errorf("op = %v, want opSelect", op)
		}
	}
}

func TestMatcher_Update(t *testing.T) {
	table, op, ok := parseSQL("UPDATE users SET name = $1 WHERE id = $2")
	if !ok {
		t.Fatal("not matched")
	}
	if table != "users" || op != opUpdate {
		t.Errorf("got (%q, %v)", table, op)
	}
}

func TestMatcher_Delete(t *testing.T) {
	table, op, ok := parseSQL("DELETE FROM users WHERE id = $1")
	if !ok {
		t.Fatal("not matched")
	}
	if table != "users" || op != opDelete {
		t.Errorf("got (%q, %v)", table, op)
	}
}

func TestMatcher_CreateTable(t *testing.T) {
	table, op, ok := parseSQL("CREATE TABLE IF NOT EXISTS users (id TEXT)")
	if !ok {
		t.Fatal("not matched")
	}
	if table != "users" || op != opCreateTable {
		t.Errorf("got (%q, %v)", table, op)
	}
}

func TestMatcher_Passthrough(t *testing.T) {
	_, _, ok := parseSQL("EXPLAIN SELECT * FROM users")
	if ok {
		t.Fatal("EXPLAIN should not match")
	}
}

func TestMatcher_JoinDetection(t *testing.T) {
	table, op, ok := parseSQL("SELECT u.*, o.* FROM users u JOIN orders o ON o.user_id = u.id")
	if !ok {
		t.Fatal("not matched")
	}
	if table != "users" || op != opSelectJoin {
		t.Errorf("got (%q, %v)", table, op)
	}
}
