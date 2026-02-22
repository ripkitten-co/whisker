package projections

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/events"
)

type Poller struct {
	store     *whisker.Store
	pool      *pgxpool.Pool
	batchSize int
}

func NewPoller(store *whisker.Store, batchSize int) *Poller {
	return &Poller{
		store:     store,
		pool:      store.PgxPool(),
		batchSize: batchSize,
	}
}

func (p *Poller) Poll(ctx context.Context, afterPosition int64) ([]events.Event, error) {
	es := events.New(p.store)
	return es.ReadAll(ctx, afterPosition, p.batchSize)
}

func (p *Poller) WaitForNotification(ctx context.Context) error {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("poller: acquire conn: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, "LISTEN whisker_events")
	if err != nil {
		return fmt.Errorf("poller: listen: %w", err)
	}

	_, err = conn.Conn().WaitForNotification(ctx)
	if err != nil {
		return fmt.Errorf("poller: wait: %w", err)
	}
	return nil
}
