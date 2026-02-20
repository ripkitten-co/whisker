package meta

import (
	"testing"
)

type conventionDoc struct {
	ID      string
	Name    string
	Email   string
	Version int
}

type taggedDoc struct {
	Key  string `whisker:"id"`
	Name string
	Rev  int `whisker:"version"`
}

type noVersionDoc struct {
	ID   string
	Name string
}

type noIDDoc struct {
	Name    string
	Version int
}

type jsonTagDoc struct {
	ID       string
	FullName string `json:"full_name"`
	Internal string `json:"-"`
	Email    string
	Version  int
}

type unexportedDoc struct {
	ID      string
	Name    string
	secret  string
	Version int
}

func TestToCamelCase(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"FirstName", "firstName"},
		{"Name", "name"},
		{"URL", "url"},
		{"HTTPStatus", "httpStatus"},
		{"ID", "id"},
		{"MyURL", "myURL"},
		{"A", "a"},
		{"alreadyCamel", "alreadyCamel"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := toCamelCase(tt.in)
			if got != tt.want {
				t.Errorf("toCamelCase(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestAnalyze_Convention(t *testing.T) {
	m := Analyze[conventionDoc]()
	if m.IDIndex != 0 {
		t.Errorf("IDIndex = %d, want 0", m.IDIndex)
	}
	if m.VersionIndex != 3 {
		t.Errorf("VersionIndex = %d, want 3", m.VersionIndex)
	}
	if len(m.Fields) != 2 {
		t.Fatalf("len(Fields) = %d, want 2", len(m.Fields))
	}
	if m.Fields[0].JSONKey != "name" {
		t.Errorf("Fields[0].JSONKey = %q, want %q", m.Fields[0].JSONKey, "name")
	}
	if m.Fields[1].JSONKey != "email" {
		t.Errorf("Fields[1].JSONKey = %q, want %q", m.Fields[1].JSONKey, "email")
	}
}

func TestAnalyze_WhiskerTags(t *testing.T) {
	m := Analyze[taggedDoc]()
	if m.IDIndex != 0 {
		t.Errorf("IDIndex = %d, want 0 (Key field)", m.IDIndex)
	}
	if m.VersionIndex != 2 {
		t.Errorf("VersionIndex = %d, want 2 (Rev field)", m.VersionIndex)
	}
	if len(m.Fields) != 1 {
		t.Fatalf("len(Fields) = %d, want 1", len(m.Fields))
	}
	if m.Fields[0].JSONKey != "name" {
		t.Errorf("Fields[0].JSONKey = %q, want %q", m.Fields[0].JSONKey, "name")
	}
}

func TestAnalyze_NoVersion(t *testing.T) {
	m := Analyze[noVersionDoc]()
	if m.IDIndex != 0 {
		t.Errorf("IDIndex = %d, want 0", m.IDIndex)
	}
	if m.VersionIndex != -1 {
		t.Errorf("VersionIndex = %d, want -1", m.VersionIndex)
	}
}

func TestAnalyze_NoID(t *testing.T) {
	m := Analyze[noIDDoc]()
	if m.IDIndex != -1 {
		t.Errorf("IDIndex = %d, want -1", m.IDIndex)
	}
	if m.VersionIndex != 1 {
		t.Errorf("VersionIndex = %d, want 1", m.VersionIndex)
	}
}

func TestAnalyze_JSONTags(t *testing.T) {
	m := Analyze[jsonTagDoc]()
	if len(m.Fields) != 2 {
		t.Fatalf("len(Fields) = %d, want 2", len(m.Fields))
	}
	if m.Fields[0].JSONKey != "full_name" {
		t.Errorf("Fields[0].JSONKey = %q, want %q", m.Fields[0].JSONKey, "full_name")
	}
	if m.Fields[1].JSONKey != "email" {
		t.Errorf("Fields[1].JSONKey = %q, want %q", m.Fields[1].JSONKey, "email")
	}
}

func TestAnalyze_UnexportedFieldsSkipped(t *testing.T) {
	m := Analyze[unexportedDoc]()
	if len(m.Fields) != 1 {
		t.Fatalf("len(Fields) = %d, want 1", len(m.Fields))
	}
	if m.Fields[0].JSONKey != "name" {
		t.Errorf("Fields[0].JSONKey = %q, want %q", m.Fields[0].JSONKey, "name")
	}
}

func TestAnalyze_Cached(t *testing.T) {
	m1 := Analyze[conventionDoc]()
	m2 := Analyze[conventionDoc]()
	if m1 != m2 {
		t.Error("expected Analyze to return the same pointer on second call (cached)")
	}
}

func TestExtractID_Convention(t *testing.T) {
	doc := &conventionDoc{ID: "abc-123", Name: "Alice"}
	id, err := ExtractID(doc)
	if err != nil {
		t.Fatalf("ExtractID: %v", err)
	}
	if id != "abc-123" {
		t.Errorf("got %q, want %q", id, "abc-123")
	}
}

func TestExtractID_Tag(t *testing.T) {
	doc := &taggedDoc{Key: "key-456", Name: "Bob"}
	id, err := ExtractID(doc)
	if err != nil {
		t.Fatalf("ExtractID: %v", err)
	}
	if id != "key-456" {
		t.Errorf("got %q, want %q", id, "key-456")
	}
}

func TestExtractID_Missing(t *testing.T) {
	doc := &noIDDoc{Name: "Alice"}
	_, err := ExtractID(doc)
	if err == nil {
		t.Fatal("expected error for missing ID field")
	}
}

func TestExtractVersion(t *testing.T) {
	doc := &conventionDoc{ID: "abc", Version: 5}
	v, ok := ExtractVersion(doc)
	if !ok {
		t.Fatal("expected version field to be found")
	}
	if v != 5 {
		t.Errorf("got %d, want 5", v)
	}
}

func TestExtractVersion_Missing(t *testing.T) {
	doc := &noVersionDoc{ID: "abc"}
	_, ok := ExtractVersion(doc)
	if ok {
		t.Fatal("expected version field to not be found")
	}
}

func TestSetVersion(t *testing.T) {
	doc := &conventionDoc{ID: "abc", Version: 1}
	SetVersion(doc, 3)
	if doc.Version != 3 {
		t.Errorf("got %d, want 3", doc.Version)
	}
}

func TestSetID(t *testing.T) {
	doc := &conventionDoc{Name: "Alice"}
	SetID(doc, "new-id")
	if doc.ID != "new-id" {
		t.Errorf("got %q, want %q", doc.ID, "new-id")
	}
}

func TestSetID_NonStringID(t *testing.T) {
	type intIDDoc struct {
		ID   int
		Name string
	}
	doc := &intIDDoc{ID: 0, Name: "Alice"}
	SetID(doc, "123")
	if doc.ID != 0 {
		t.Error("SetID should be no-op for non-string ID fields")
	}
}
