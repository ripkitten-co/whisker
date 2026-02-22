package whisker

import "github.com/ripkitten-co/whisker/internal/codecs"

// Option configures a Store during creation.
type Option func(*storeConfig)

type storeConfig struct {
	codec codecs.Codec
}

func defaultConfig() *storeConfig {
	return &storeConfig{
		codec: codecs.NewJSONIter(),
	}
}

// WithCodec overrides the default JSON codec (jsoniter).
func WithCodec(c codecs.Codec) Option {
	return func(cfg *storeConfig) {
		cfg.codec = c
	}
}
