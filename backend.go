package whisker

import (
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

type backend struct {
	exec   pg.Executor
	codec  codecs.Codec
	schema *schema.Bootstrap
}

type Backend interface {
	whiskerBackend() *backend
}
