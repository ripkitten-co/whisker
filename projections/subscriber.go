package projections

import (
	"context"

	"github.com/ripkitten-co/whisker/events"
)

// Subscriber is implemented by both read-model projections and side-effect
// handlers. The daemon dispatches events to each subscriber independently.
type Subscriber interface {
	Name() string
	EventTypes() []string
	Process(ctx context.Context, evts []events.Event, store ProcessingStore) error
}

// ProcessingStore abstracts read-model persistence so projections don't depend
// on the documents package directly. Side-effect handlers ignore it.
type ProcessingStore interface {
	LoadState(ctx context.Context, collection, id string) ([]byte, int, error)
	UpsertState(ctx context.Context, collection, id string, data []byte, version int) error
	DeleteState(ctx context.Context, collection, id string) error
}
