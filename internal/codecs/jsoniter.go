package codecs

import jsoniter "github.com/json-iterator/go"

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// JSONIterCodec uses jsoniter for fast JSON marshaling compatible with
// encoding/json.
type JSONIterCodec struct{}

// NewJSONIter returns a jsoniter-based codec.
func NewJSONIter() *JSONIterCodec {
	return &JSONIterCodec{}
}

func (c *JSONIterCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JSONIterCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
