package projections

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/events"
)

// Poller reads batches of events from the event store and supports
// LISTEN/NOTIFY for low-latency wakeups.
type Poller struct {
	store     *whisker.Store
	pool      *pgxpool.Pool
	batchSize int
}

// NewPoller creates a poller that reads up to batchSize events per poll.
func NewPoller(store *whisker.Store, batchSize int) *Poller {
	return &Poller{
		store:     store,
		pool:      store.PgxPool(),
		batchSize: batchSize,
	}
}

// Poll returns events with global_position greater than afterPosition.
func (p *Poller) Poll(ctx context.Context, afterPosition int64) ([]events.Event, error) {
	es := events.New(p.store)
	return es.ReadAll(ctx, afterPosition, p.batchSize)
}

// WaitForNotification blocks until a NOTIFY arrives on the whisker_events
// channel or the context is cancelled.
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
