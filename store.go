package whisker

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ripkitten-co/whisker/internal/codecs"
	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

// Store is the main entry point for Whisker. It holds a PostgreSQL connection
// pool and provides access to document collections, event streams, and sessions.
type Store struct {
	pool *pg.Pool
	be   backend
}

// New connects to PostgreSQL and returns a configured Store.
func New(ctx context.Context, connString string, opts ...Option) (*Store, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}

	pool, err := pg.NewPool(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("whisker: %w", err)
	}

	s := &Store{
		pool: pool,
		be: backend{
			exec:   pool,
			codec:  codecs.NewWhisker(cfg.codec),
			schema: schema.New(),
		},
	}
	return s, nil
}

// Close shuts down the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// DBExecutor returns the underlying database executor.
func (s *Store) DBExecutor() pg.Executor { return s.be.exec }

// JSONCodec returns the configured JSON codec.
func (s *Store) JSONCodec() codecs.Codec { return s.be.codec }

// SchemaBootstrap returns the schema bootstrap manager.
func (s *Store) SchemaBootstrap() *schema.Bootstrap { return s.be.schema }

// PgxPool returns the underlying pgxpool.Pool for use with stdlib adapters.
func (s *Store) PgxPool() *pgxpool.Pool { return s.pool.PgxPool() }
