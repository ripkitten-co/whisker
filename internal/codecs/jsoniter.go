package codecs

import jsoniter "github.com/json-iterator/go"

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type JSONIterCodec struct{}

func NewJSONIter() *JSONIterCodec {
	return &JSONIterCodec{}
}

func (c *JSONIterCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JSONIterCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
