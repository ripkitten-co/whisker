package hooks

import (
	"context"
	"sync"

	"github.com/ripkitten-co/whisker"
)

// Pool wraps a Whisker store and presents a pgx-compatible query interface.
// SQL from ORMs targeting registered models is rewritten to use Whisker's
// JSONB document storage. Unregistered queries pass through unchanged.
type Pool struct {
	store   *whisker.Store
	reg     *registry
	ensured map[string]struct{}
	mu      sync.Mutex
}

// NewPool creates a middleware pool backed by the given store.
func NewPool(store *whisker.Store) *Pool {
	return &Pool{
		store:   store,
		reg:     newRegistry(),
		ensured: make(map[string]struct{}),
	}
}

// Register teaches the pool about a model so its SQL can be intercepted.
func Register[T any](p *Pool, name string) {
	p.reg.register(name, analyzeModel[T](name))
}

func (p *Pool) ensureTable(ctx context.Context, info *modelInfo) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.ensured[info.table]; ok {
		return nil
	}

	ddl, err := rewriteCreateTable(info, "")
	if err != nil {
		return err
	}

	if _, err := p.store.DBExecutor().Exec(ctx, ddl); err != nil {
		return err
	}

	p.ensured[info.table] = struct{}{}
	return nil
}
