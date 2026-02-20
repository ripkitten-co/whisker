package codecs_test

import (
	"encoding/json"
	"testing"

	"github.com/ripkitten-co/whisker/internal/codecs"
)

type testDoc struct {
	ID      string
	Name    string
	Email   string
	Version int
}

type tagOverrideDoc struct {
	ID      string
	Name    string `json:"full_name"`
	Secret  string `json:"-"`
	Email   string
	Version int
}

type camelDoc struct {
	ID        string
	FirstName string
	LastName  string
	Version   int
}

type numericDoc struct {
	ID      string
	Count   int
	Score   float64
	Version int
}

func newWhisker() codecs.Codec {
	return codecs.NewWhisker(codecs.NewJSONIter())
}

func TestWhiskerCodec_Marshal_ExcludesIDAndVersion(t *testing.T) {
	c := newWhisker()
	doc := testDoc{ID: "abc", Name: "Alice", Email: "alice@test.com", Version: 3}

	data, err := c.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse json: %v", err)
	}

	for _, key := range []string{"ID", "id", "Version", "version"} {
		if _, ok := raw[key]; ok {
			t.Errorf("unexpected key %q in marshalled output", key)
		}
	}

	if _, ok := raw["name"]; !ok {
		t.Error("missing key \"name\"")
	}
	if _, ok := raw["email"]; !ok {
		t.Error("missing key \"email\"")
	}
}

func TestWhiskerCodec_Marshal_CamelCaseKeys(t *testing.T) {
	c := newWhisker()
	doc := camelDoc{ID: "1", FirstName: "Jane", LastName: "Doe", Version: 1}

	data, err := c.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse json: %v", err)
	}

	if _, ok := raw["firstName"]; !ok {
		t.Error("missing key \"firstName\"")
	}
	if _, ok := raw["lastName"]; !ok {
		t.Error("missing key \"lastName\"")
	}
}

func TestWhiskerCodec_Marshal_RespectsJSONTags(t *testing.T) {
	c := newWhisker()
	doc := tagOverrideDoc{
		ID: "1", Name: "Alice", Secret: "s3cret", Email: "a@b.com", Version: 1,
	}

	data, err := c.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse json: %v", err)
	}

	if _, ok := raw["full_name"]; !ok {
		t.Error("missing key \"full_name\"")
	}
	for _, key := range []string{"Secret", "secret"} {
		if _, ok := raw[key]; ok {
			t.Errorf("unexpected key %q (json:\"-\" field should be excluded)", key)
		}
	}
}

func TestWhiskerCodec_Unmarshal_PopulatesDataFields(t *testing.T) {
	c := newWhisker()
	input := []byte(`{"name":"Alice","email":"alice@test.com"}`)

	var doc testDoc
	if err := c.Unmarshal(input, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if doc.Name != "Alice" {
		t.Errorf("Name = %q, want %q", doc.Name, "Alice")
	}
	if doc.Email != "alice@test.com" {
		t.Errorf("Email = %q, want %q", doc.Email, "alice@test.com")
	}
	if doc.ID != "" {
		t.Errorf("ID = %q, want zero value", doc.ID)
	}
	if doc.Version != 0 {
		t.Errorf("Version = %d, want 0", doc.Version)
	}
}

func TestWhiskerCodec_Unmarshal_IgnoresUnknownKeys(t *testing.T) {
	c := newWhisker()
	input := []byte(`{"name":"Alice","email":"a@b.com","unknown":"value"}`)

	var doc testDoc
	if err := c.Unmarshal(input, &doc); err != nil {
		t.Fatalf("unmarshal: unexpected error: %v", err)
	}

	if doc.Name != "Alice" {
		t.Errorf("Name = %q, want %q", doc.Name, "Alice")
	}
}

func TestWhiskerCodec_Unmarshal_RespectsJSONTags(t *testing.T) {
	c := newWhisker()
	input := []byte(`{"full_name":"Alice","email":"a@b.com"}`)

	var doc tagOverrideDoc
	if err := c.Unmarshal(input, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if doc.Name != "Alice" {
		t.Errorf("Name = %q, want %q", doc.Name, "Alice")
	}
	if doc.Email != "a@b.com" {
		t.Errorf("Email = %q, want %q", doc.Email, "a@b.com")
	}
}

func TestWhiskerCodec_RoundTrip(t *testing.T) {
	c := newWhisker()
	original := testDoc{ID: "xyz", Name: "Bob", Email: "bob@test.com", Version: 5}

	data, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got testDoc
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Name != original.Name {
		t.Errorf("Name = %q, want %q", got.Name, original.Name)
	}
	if got.Email != original.Email {
		t.Errorf("Email = %q, want %q", got.Email, original.Email)
	}
	// ID and Version should NOT survive the round trip
	if got.ID != "" {
		t.Errorf("ID = %q, want zero value after round trip", got.ID)
	}
	if got.Version != 0 {
		t.Errorf("Version = %d, want 0 after round trip", got.Version)
	}
}

func TestWhiskerCodec_MarshalNumericFields(t *testing.T) {
	c := newWhisker()
	doc := numericDoc{ID: "1", Count: 42, Score: 3.14, Version: 1}

	data, err := c.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse json: %v", err)
	}

	if string(raw["count"]) != "42" {
		t.Errorf("count = %s, want 42", raw["count"])
	}
	if string(raw["score"]) != "3.14" {
		t.Errorf("score = %s, want 3.14", raw["score"])
	}
}

func TestWhiskerCodec_UnmarshalNumericFields(t *testing.T) {
	c := newWhisker()
	input := []byte(`{"count":42,"score":3.14}`)

	var doc numericDoc
	if err := c.Unmarshal(input, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if doc.Count != 42 {
		t.Errorf("Count = %d, want 42", doc.Count)
	}
	if doc.Score != 3.14 {
		t.Errorf("Score = %f, want 3.14", doc.Score)
	}
}
