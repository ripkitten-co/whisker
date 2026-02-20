package whisker

import "testing"

type taggedDoc struct {
	ID      string `whisker:"id"`
	Name    string `json:"name"`
	Version int    `whisker:"version"`
}

type noVersionDoc struct {
	ID   string `whisker:"id"`
	Name string `json:"name"`
}

type noIDDoc struct {
	Name string `json:"name"`
}

func TestExtractID(t *testing.T) {
	doc := &taggedDoc{ID: "abc", Name: "Alice", Version: 1}
	id, err := extractID(doc)
	if err != nil {
		t.Fatalf("extractID: %v", err)
	}
	if id != "abc" {
		t.Errorf("got %q, want %q", id, "abc")
	}
}

func TestExtractID_Missing(t *testing.T) {
	doc := &noIDDoc{Name: "Alice"}
	_, err := extractID(doc)
	if err == nil {
		t.Fatal("expected error for missing whisker:\"id\" tag")
	}
}

func TestExtractVersion(t *testing.T) {
	doc := &taggedDoc{ID: "abc", Name: "Alice", Version: 5}
	v, ok := extractVersion(doc)
	if !ok {
		t.Fatal("expected version field to be found")
	}
	if v != 5 {
		t.Errorf("got %d, want 5", v)
	}
}

func TestExtractVersion_Missing(t *testing.T) {
	doc := &noVersionDoc{ID: "abc", Name: "Alice"}
	_, ok := extractVersion(doc)
	if ok {
		t.Fatal("expected version field to not be found")
	}
}

func TestSetVersion(t *testing.T) {
	doc := &taggedDoc{ID: "abc", Version: 1}
	setVersion(doc, 3)
	if doc.Version != 3 {
		t.Errorf("got %d, want 3", doc.Version)
	}
}
