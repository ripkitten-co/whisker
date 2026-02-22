package projections

import (
	"context"
	"fmt"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/events"
)

// ApplyFunc is the callback signature for read-model projections. It receives
// the current state (nil for the first event on a stream) and returns the new
// state. Returning nil deletes the read model for that stream.
type ApplyFunc[T any] func(ctx context.Context, evt events.Event, state *T) (*T, error)

// Projection builds a read model from event streams. Register event handlers
// with On, then add the projection to a Daemon for continuous processing.
type Projection[T any] struct {
	name     string
	store    *whisker.Store
	handlers map[string]ApplyFunc[T]
}

// New creates a projection that writes to the whisker_{name} collection.
func New[T any](store *whisker.Store, name string) *Projection[T] {
	return &Projection[T]{
		name:     name,
		store:    store,
		handlers: make(map[string]ApplyFunc[T]),
	}
}

// On registers a handler for the given event type. Returns the projection
// for method chaining.
func (p *Projection[T]) On(eventType string, fn ApplyFunc[T]) *Projection[T] {
	p.handlers[eventType] = fn
	return p
}

// Name returns the projection identifier, used for checkpointing and table naming.
func (p *Projection[T]) Name() string {
	return p.name
}

// EventTypes returns the event types this projection handles.
func (p *Projection[T]) EventTypes() []string {
	types := make([]string, 0, len(p.handlers))
	for t := range p.handlers {
		types = append(types, t)
	}
	return types
}

// Process applies matching events to the read model. For each event it loads
// current state, calls the registered handler, then upserts or deletes the
// result.
func (p *Projection[T]) Process(ctx context.Context, evts []events.Event, ps ProcessingStore) error {
	codec := p.store.JSONCodec()
	for _, evt := range evts {
		fn, ok := p.handlers[evt.Type]
		if !ok {
			continue
		}

		var state *T
		data, version, err := ps.LoadState(ctx, p.name, evt.StreamID)
		if err == nil && data != nil {
			state = new(T)
			if err := codec.Unmarshal(data, state); err != nil {
				return fmt.Errorf("projection %s: unmarshal state for %s: %w", p.name, evt.StreamID, err)
			}
		}

		result, err := fn(ctx, evt, state)
		if err != nil {
			return fmt.Errorf("projection %s: handle %s for %s: %w", p.name, evt.Type, evt.StreamID, err)
		}

		if result == nil {
			if err := ps.DeleteState(ctx, p.name, evt.StreamID); err != nil {
				return fmt.Errorf("projection %s: delete state for %s: %w", p.name, evt.StreamID, err)
			}
			continue
		}

		out, err := codec.Marshal(result)
		if err != nil {
			return fmt.Errorf("projection %s: marshal state for %s: %w", p.name, evt.StreamID, err)
		}
		if err := ps.UpsertState(ctx, p.name, evt.StreamID, out, version); err != nil {
			return fmt.Errorf("projection %s: upsert state for %s: %w", p.name, evt.StreamID, err)
		}
	}
	return nil
}
