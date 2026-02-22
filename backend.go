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

// Backend provides access to the core Whisker subsystems: database executor,
// JSON codec, and schema bootstrap. Both Store and Session implement it.
type Backend interface {
	DBExecutor() pg.Executor
	JSONCodec() codecs.Codec
	SchemaBootstrap() *schema.Bootstrap
}
