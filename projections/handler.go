package projections

import (
	"context"
	"fmt"

	"github.com/ripkitten-co/whisker/events"
)

// HandleFunc is the callback signature for side-effect handlers. Unlike
// ApplyFunc, it has no state — just react to the event and return.
type HandleFunc func(ctx context.Context, evt events.Event) error

// Handler reacts to events for side effects (sending emails, calling APIs)
// without maintaining a read model. It shares checkpoint infrastructure with
// Projection for at-least-once delivery.
type Handler struct {
	name     string
	handlers map[string]HandleFunc
}

// NewHandler creates a side-effect handler with the given name.
func NewHandler(name string) *Handler {
	return &Handler{
		name:     name,
		handlers: make(map[string]HandleFunc),
	}
}

// On registers a handler for the given event type. Returns the handler for
// method chaining.
func (h *Handler) On(eventType string, fn HandleFunc) *Handler {
	h.handlers[eventType] = fn
	return h
}

// Name returns the handler identifier, used for checkpointing.
func (h *Handler) Name() string {
	return h.name
}

// EventTypes returns the event types this handler responds to.
func (h *Handler) EventTypes() []string {
	types := make([]string, 0, len(h.handlers))
	for t := range h.handlers {
		types = append(types, t)
	}
	return types
}

// Process calls registered handlers for matching events. The ProcessingStore
// argument is ignored since side-effect handlers don't maintain state.
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
