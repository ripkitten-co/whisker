package codecs

// Codec marshals and unmarshals values to and from bytes.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}
