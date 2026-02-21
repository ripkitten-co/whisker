package meta

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

type StructMeta struct {
	IDIndex      int
	VersionIndex int
	Fields       []FieldMeta
}

type FieldMeta struct {
	Index   int
	JSONKey string
}

var cache sync.Map

func Analyze[T any]() *StructMeta {
	return AnalyzeType(reflect.TypeOf((*T)(nil)).Elem())
}

func AnalyzeType(t reflect.Type) *StructMeta {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if cached, ok := cache.Load(t); ok {
		return cached.(*StructMeta)
	}
	m := analyze(t)
	actual, _ := cache.LoadOrStore(t, m)
	return actual.(*StructMeta)
}

func analyze(t reflect.Type) *StructMeta {
	m := &StructMeta{IDIndex: -1, VersionIndex: -1}
	applyWhiskerTags(t, m)
	applyConventionDefaults(t, m)
	collectDataFields(t, m)
	return m
}

func applyWhiskerTags(t reflect.Type, m *StructMeta) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		switch f.Tag.Get("whisker") {
		case "id":
			m.IDIndex = i
		case "version":
			m.VersionIndex = i
		}
	}
}

func applyConventionDefaults(t reflect.Type, m *StructMeta) {
	if m.IDIndex == -1 {
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).Name == "ID" {
				m.IDIndex = i
				break
			}
		}
	}
	if m.VersionIndex == -1 {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.Name == "Version" && f.Type.Kind() == reflect.Int {
				m.VersionIndex = i
				break
			}
		}
	}
}

func collectDataFields(t reflect.Type, m *StructMeta) {
	for i := 0; i < t.NumField(); i++ {
		if i == m.IDIndex || i == m.VersionIndex {
			continue
		}
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		jsonTag := f.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}

		key := jsonKeyFromTag(jsonTag)
		if key == "" {
			key = toCamelCase(f.Name)
		}
		m.Fields = append(m.Fields, FieldMeta{Index: i, JSONKey: key})
	}
}

func jsonKeyFromTag(tag string) string {
	if tag == "" {
		return ""
	}
	name, _, _ := strings.Cut(tag, ",")
	return name
}

func toCamelCase(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	if unicode.IsLower(runes[0]) {
		return s
	}

	// find the length of the leading uppercase run
	upper := 0
	for _, r := range runes {
		if !unicode.IsUpper(r) {
			break
		}
		upper++
	}

	// entire string is uppercase (e.g. "ID", "URL")
	if upper == len(runes) {
		return strings.ToLower(s)
	}

	// single leading capital (e.g. "Name" -> "name")
	if upper == 1 {
		return string(unicode.ToLower(runes[0])) + string(runes[1:])
	}

	// multi-char uppercase prefix (e.g. "HTTPStatus" -> "httpStatus")
	// lowercase all but the last uppercase char, which starts the next word
	return strings.ToLower(string(runes[:upper-1])) + string(runes[upper-1:])
}

func analyzeValue(doc any) (reflect.Value, *StructMeta) {
	v := reflect.ValueOf(doc)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v, AnalyzeType(v.Type())
}

func ExtractID(doc any) (string, error) {
	v, m := analyzeValue(doc)
	if m.IDIndex == -1 {
		return "", fmt.Errorf("whisker: no ID field in %s", v.Type().Name())
	}
	return fmt.Sprint(v.Field(m.IDIndex).Interface()), nil
}

func ExtractVersion(doc any) (int, bool) {
	v, m := analyzeValue(doc)
	if m.VersionIndex == -1 {
		return 0, false
	}
	return int(v.Field(m.VersionIndex).Int()), true
}

func SetVersion(doc any, version int) {
	v, m := analyzeValue(doc)
	if m.VersionIndex == -1 {
		return
	}
	v.Field(m.VersionIndex).SetInt(int64(version))
}

func SetID(doc any, id string) {
	v, m := analyzeValue(doc)
	if m.IDIndex == -1 {
		return
	}
	f := v.Field(m.IDIndex)
	if f.Type().Kind() != reflect.String {
		return
	}
	f.SetString(id)
}
