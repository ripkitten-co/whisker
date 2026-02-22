package whisker

import "github.com/ripkitten-co/whisker/internal/codecs"

// Option configures a Store during creation.
type Option func(*storeConfig)

type storeConfig struct {
	codec        codecs.Codec
	maxBatchSize int
}

func defaultConfig() *storeConfig {
	return &storeConfig{
		codec:        codecs.NewJSONIter(),
		maxBatchSize: 1000,
	}
}

// WithCodec overrides the default JSON codec (jsoniter).
func WithCodec(c codecs.Codec) Option {
	return func(cfg *storeConfig) {
		cfg.codec = c
	}
}

// WithMaxBatchSize sets the maximum number of documents per batch operation.
func WithMaxBatchSize(n int) Option {
	return func(cfg *storeConfig) {
		cfg.maxBatchSize = n
	}
}
