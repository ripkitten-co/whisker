package codecs

import (
	stdjson "encoding/json"
	"fmt"
	"reflect"

	"github.com/ripkitten-co/whisker/internal/meta"
)

type WhiskerCodec struct {
	inner Codec
}

func NewWhisker(inner Codec) *WhiskerCodec {
	return &WhiskerCodec{inner: inner}
}

func (c *WhiskerCodec) Marshal(v any) ([]byte, error) {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	m := meta.AnalyzeType(val.Type())

	out := make(map[string]any, len(m.Fields))
	for _, f := range m.Fields {
		out[f.JSONKey] = val.Field(f.Index).Interface()
	}

	return c.inner.Marshal(out)
}

func (c *WhiskerCodec) Unmarshal(data []byte, v any) error {
	var raw map[string]stdjson.RawMessage
	if err := c.inner.Unmarshal(data, &raw); err != nil {
		return err
	}

	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	m := meta.AnalyzeType(val.Type())

	for _, f := range m.Fields {
		rawVal, ok := raw[f.JSONKey]
		if !ok {
			continue
		}
		fieldPtr := reflect.New(val.Field(f.Index).Type())
		if err := c.inner.Unmarshal(rawVal, fieldPtr.Interface()); err != nil {
			return fmt.Errorf("field %s: %w", f.JSONKey, err)
		}
		val.Field(f.Index).Set(fieldPtr.Elem())
	}

	return nil
}
