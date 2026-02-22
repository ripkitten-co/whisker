package projections

import (
	"context"

	"github.com/ripkitten-co/whisker/events"
)

type Subscriber interface {
	Name() string
	EventTypes() []string
	Process(ctx context.Context, evts []events.Event, store ProcessingStore) error
}

type ProcessingStore interface {
	LoadState(ctx context.Context, collection, id string) ([]byte, int, error)
	UpsertState(ctx context.Context, collection, id string, data []byte, version int) error
	DeleteState(ctx context.Context, collection, id string) error
}
