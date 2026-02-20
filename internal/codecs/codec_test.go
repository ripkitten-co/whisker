package codecs_test

import (
	"testing"

	"github.com/ripkitten-co/whisker/internal/codecs"
)

type sample struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestJSONIterCodec_Roundtrip(t *testing.T) {
	c := codecs.NewJSONIter()

	original := sample{Name: "Alice", Age: 30}
	data, err := c.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got sample
	if err := c.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got != original {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

func TestJSONIterCodec_MarshalProducesJSON(t *testing.T) {
	c := codecs.NewJSONIter()

	data, err := c.Marshal(sample{Name: "Bob", Age: 25})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	s := string(data)
	if s != `{"name":"Bob","age":25}` {
		t.Errorf("got %s", s)
	}
}

func TestJSONIterCodec_UnmarshalError(t *testing.T) {
	c := codecs.NewJSONIter()

	var got sample
	err := c.Unmarshal([]byte("not json"), &got)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}
