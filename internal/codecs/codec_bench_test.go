package codecs

import "testing"

type smallDoc struct {
	Name  string
	Email string
}

type mediumDoc struct {
	Name    string
	Email   string
	Bio     string
	Address string
	Phone   string
	Company string
	Title   string
	Website string
}

type largeDoc struct {
	Name     string
	Email    string
	Bio      string
	Address  string
	Phone    string
	Company  string
	Title    string
	Website  string
	Tags     []string
	Metadata map[string]string
	Notes    string
	Country  string
	City     string
	State    string
	Zip      string
	Avatar   string
}

func BenchmarkJSONIter_Marshal_Small(b *testing.B) {
	c := NewJSONIter()
	doc := smallDoc{Name: "Alice", Email: "alice@test.com"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = c.Marshal(doc)
	}
}

func BenchmarkJSONIter_Marshal_Medium(b *testing.B) {
	c := NewJSONIter()
	doc := mediumDoc{
		Name: "Alice", Email: "alice@test.com", Bio: "Software engineer",
		Address: "123 Main St", Phone: "555-1234", Company: "Acme",
		Title: "Senior Engineer", Website: "https://alice.dev",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = c.Marshal(doc)
	}
}

func BenchmarkJSONIter_Marshal_Large(b *testing.B) {
	c := NewJSONIter()
	doc := largeDoc{
		Name: "Alice", Email: "alice@test.com", Bio: "Software engineer",
		Address: "123 Main St", Phone: "555-1234", Company: "Acme",
		Title: "Senior Engineer", Website: "https://alice.dev",
		Tags:     []string{"go", "postgres", "backend"},
		Metadata: map[string]string{"team": "platform", "role": "lead"},
		Notes:    "Key contributor", Country: "SE", City: "Stockholm",
		State: "Stockholm", Zip: "111 22", Avatar: "https://img.test/alice.png",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = c.Marshal(doc)
	}
}

func BenchmarkJSONIter_Unmarshal_Small(b *testing.B) {
	c := NewJSONIter()
	data, _ := c.Marshal(smallDoc{Name: "Alice", Email: "alice@test.com"})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var doc smallDoc
		_ = c.Unmarshal(data, &doc)
	}
}

func BenchmarkJSONIter_Unmarshal_Medium(b *testing.B) {
	c := NewJSONIter()
	data, _ := c.Marshal(mediumDoc{
		Name: "Alice", Email: "alice@test.com", Bio: "Software engineer",
		Address: "123 Main St", Phone: "555-1234", Company: "Acme",
		Title: "Senior Engineer", Website: "https://alice.dev",
	})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var doc mediumDoc
		_ = c.Unmarshal(data, &doc)
	}
}

func BenchmarkJSONIter_Unmarshal_Large(b *testing.B) {
	c := NewJSONIter()
	data, _ := c.Marshal(largeDoc{
		Name: "Alice", Email: "alice@test.com", Bio: "Software engineer",
		Address: "123 Main St", Phone: "555-1234", Company: "Acme",
		Title: "Senior Engineer", Website: "https://alice.dev",
		Tags:     []string{"go", "postgres", "backend"},
		Metadata: map[string]string{"team": "platform", "role": "lead"},
		Notes:    "Key contributor", Country: "SE", City: "Stockholm",
		State: "Stockholm", Zip: "111 22", Avatar: "https://img.test/alice.png",
	})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var doc largeDoc
		_ = c.Unmarshal(data, &doc)
	}
}

func BenchmarkWhisker_Marshal(b *testing.B) {
	type doc struct {
		ID      string
		Name    string
		Email   string
		Version int
	}
	c := NewWhisker(NewJSONIter())
	d := doc{ID: "u1", Name: "Alice", Email: "alice@test.com", Version: 3}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _ = c.Marshal(d)
	}
}

func BenchmarkWhisker_Unmarshal(b *testing.B) {
	type doc struct {
		ID      string
		Name    string
		Email   string
		Version int
	}
	c := NewWhisker(NewJSONIter())
	data, _ := c.Marshal(doc{ID: "u1", Name: "Alice", Email: "alice@test.com", Version: 3})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var d doc
		_ = c.Unmarshal(data, &d)
	}
}
