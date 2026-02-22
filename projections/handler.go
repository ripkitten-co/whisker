package projections

import (
	"context"
	"fmt"

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

func (h *Handler) Process(ctx context.Context, evts []events.Event, _ ProcessingStore) error {
	for _, evt := range evts {
		fn, ok := h.handlers[evt.Type]
		if !ok {
			continue
		}
		if err := fn(ctx, evt); err != nil {
			return fmt.Errorf("handler %s: handle %s: %w", h.name, evt.Type, err)
		}
	}
	return nil
}
