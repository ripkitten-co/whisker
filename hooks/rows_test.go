package hooks

import (
	"testing"
)

func TestUnpackJSONB(t *testing.T) {
	r := newRegistry()
	r.register("users", analyzeModel[testUser]("users"))
	info, _ := r.lookup("users")

	jsonData := []byte(`{"name":"Alice","email":"alice@test.com"}`)
	id := "u1"
	version := 1

	cols := unpackRow(info, id, jsonData, version)
	if len(cols) != 4 {
		t.Fatalf("cols = %d, want 4 (id, name, email, version)", len(cols))
	}
	if cols["id"] != "u1" {
		t.Errorf("id = %v", cols["id"])
	}
	if cols["name"] != "Alice" {
		t.Errorf("name = %v", cols["name"])
	}
	if cols["email"] != "alice@test.com" {
		t.Errorf("email = %v", cols["email"])
	}
	if cols["version"] != 1 {
		t.Errorf("version = %v", cols["version"])
	}
}
