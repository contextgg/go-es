package es

import (
	"context"
)

// NewLocalEventHandler turns an
func NewLocalEventHandler(registry EventRegistry) *LocalEventHandler {
	return &LocalEventHandler{
		registry: registry,
	}
}

// LocalEventHandler for local event handling
type LocalEventHandler struct {
	registry EventRegistry
	handlers []EventHandler
}

func (s *LocalEventHandler) AddHandler(handler EventHandler) {
	s.handlers = append(s.handlers, handler)
}

func (s *LocalEventHandler) HandleEvent(ctx context.Context, evt *Event) error {
	matcher := MatchAnyInRegistry(s.registry)
	if !matcher(evt) {
		return nil
	}
	for _, h := range s.handlers {
		if err := h.HandleEvent(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}
