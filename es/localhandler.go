package es

import (
	"context"
)

// NewLocalEventHandler turns an
func NewLocalEventHandler(registry EventRegistry, handlers []EventHandler) EventHandler {
	matcher := MatchAnyInRegistry(registry)
	return &localEventHandler{matcher, handlers}
}

type localEventHandler struct {
	matcher  EventMatcher
	handlers []EventHandler
}

func (s *localEventHandler) HandleEvent(ctx context.Context, evt *Event) error {
	if !s.matcher(evt) {
		return nil
	}
	for _, h := range s.handlers {
		if err := h.HandleEvent(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}
