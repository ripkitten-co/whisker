package whisker

import "github.com/ripkitten-co/whisker/internal/codecs"

type Option func(*storeConfig)

type storeConfig struct {
	codec codecs.Codec
}

func defaultConfig() *storeConfig {
	return &storeConfig{
		codec: codecs.NewJSONIter(),
	}
}

func WithCodec(c codecs.Codec) Option {
	return func(cfg *storeConfig) {
		cfg.codec = c
	}
}
