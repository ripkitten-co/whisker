package projections

import (
	"context"
	"fmt"

	"github.com/ripkitten-co/whisker"
	"github.com/ripkitten-co/whisker/events"
)

type ApplyFunc[T any] func(ctx context.Context, evt events.Event, state *T) (*T, error)

type Projection[T any] struct {
	name     string
	store    *whisker.Store
	handlers map[string]ApplyFunc[T]
}

func New[T any](store *whisker.Store, name string) *Projection[T] {
	return &Projection[T]{
		name:     name,
		store:    store,
		handlers: make(map[string]ApplyFunc[T]),
	}
}

func (p *Projection[T]) On(eventType string, fn ApplyFunc[T]) *Projection[T] {
	p.handlers[eventType] = fn
	return p
}

func (p *Projection[T]) Name() string {
	return p.name
}

func (p *Projection[T]) EventTypes() []string {
	types := make([]string, 0, len(p.handlers))
	for t := range p.handlers {
		types = append(types, t)
	}
	return types
}

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
