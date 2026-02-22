package hooks

import (
	"testing"
)

type testUser struct {
	ID      string
	Name    string
	Email   string
	Version int
}

func TestRegister_StoresModelInfo(t *testing.T) {
	r := newRegistry()
	r.register("users", analyzeModel[testUser]("users"))

	info, ok := r.lookup("users")
	if !ok {
		t.Fatal("expected model info for 'users'")
	}
	if info.table != "whisker_users" {
		t.Errorf("table = %q, want whisker_users", info.table)
	}
	if info.idColumn != "id" {
		t.Errorf("idColumn = %q, want id", info.idColumn)
	}
	if len(info.dataCols) != 2 {
		t.Errorf("dataCols = %d, want 2 (name, email)", len(info.dataCols))
	}
}

func TestRegister_LookupByWhiskerTable(t *testing.T) {
	r := newRegistry()
	r.register("users", analyzeModel[testUser]("users"))

	info, ok := r.lookupByTable("whisker_users")
	if !ok {
		t.Fatal("expected model info for whisker_users")
	}
	if info.name != "users" {
		t.Errorf("name = %q, want users", info.name)
	}
}

func TestRegister_UnknownReturnsNotFound(t *testing.T) {
	r := newRegistry()
	_, ok := r.lookup("unknown")
	if ok {
		t.Fatal("expected not found for unregistered model")
	}
}

func TestToLowerSnake(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"Name", "name"},
		{"ID", "id"},
		{"Email", "email"},
		{"HTTPStatus", "http_status"},
		{"UserID", "user_id"},
		{"URL", "url"},
		{"CreatedAt", "created_at"},
		{"A", "a"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := toLowerSnake(tt.in); got != tt.want {
			t.Errorf("toLowerSnake(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
