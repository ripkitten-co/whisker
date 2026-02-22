package hooks

import (
	"testing"
)

func TestRewrite_Insert(t *testing.T) {
	r := newRegistry()
	r.register("users", analyzeModel[testUser]("users"))

	info, _ := r.lookup("users")
	sql := "INSERT INTO users (id,name,email,version) VALUES ($1,$2,$3,$4)"
	args := []any{"u1", "Alice", "alice@test.com", 0}

	rewritten, newArgs, err := rewriteInsert(info, sql, args)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if !containsSubstring(rewritten, "whisker_users") {
		t.Errorf("expected whisker_users in SQL: %s", rewritten)
	}
	if !containsSubstring(rewritten, "jsonb_build_object") {
		t.Errorf("expected jsonb_build_object in SQL: %s", rewritten)
	}
	if len(newArgs) < 1 {
		t.Errorf("expected at least 1 arg, got %d", len(newArgs))
	}
	if newArgs[0] != "u1" {
		t.Errorf("first arg = %v, want u1", newArgs[0])
	}
}

func TestRewrite_Insert_PreservesID(t *testing.T) {
	r := newRegistry()
	r.register("users", analyzeModel[testUser]("users"))

	info, _ := r.lookup("users")
	sql := "INSERT INTO users (id,name,email) VALUES ($1,$2,$3)"
	args := []any{"u1", "Alice", "alice@test.com"}

	rewritten, newArgs, err := rewriteInsert(info, sql, args)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if newArgs[0] != "u1" {
		t.Errorf("id arg = %v, want u1", newArgs[0])
	}
	_ = rewritten
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && stringContains(s, sub))
}

func stringContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
