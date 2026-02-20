package whisker

import (
	"context"
	"fmt"

	"github.com/ripkitten-co/whisker/internal/pg"
	"github.com/ripkitten-co/whisker/schema"
)

type Store struct {
	pool *pg.Pool
	be   backend
}

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
			codec:  cfg.codec,
			schema: schema.New(),
		},
	}
	return s, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) whiskerBackend() *backend {
	return &s.be
}
