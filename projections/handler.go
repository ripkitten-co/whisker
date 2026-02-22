package projections

import (
	"context"

	"github.com/ripkitten-co/whisker/events"
)

type HandleFunc func(ctx context.Context, evt events.Event) error

type Handler struct {
	name     string
	handlers map[string]HandleFunc
}

func NewHandler(name string) *Handler {
	return &Handler{
		name:     name,
		handlers: make(map[string]HandleFunc),
	}
}

func (h *Handler) On(eventType string, fn HandleFunc) *Handler {
	h.handlers[eventType] = fn
	return h
}

func (h *Handler) Name() string {
	return h.name
}

func (h *Handler) EventTypes() []string {
	types := make([]string, 0, len(h.handlers))
	for t := range h.handlers {
		types = append(types, t)
	}
	return types
}
