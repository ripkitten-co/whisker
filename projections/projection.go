package projections

import (
	"context"

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
