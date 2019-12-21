package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewLocalHandler turns an
func NewLocalHandler(registry es.EventRegistry, handlers []es.EventHandler) es.EventHandler {
	matcher := es.MatchAnyInRegistry(registry)
	return &localHandler{matcher, handlers}
}

type localHandler struct {
	matcher  es.EventMatcher
	handlers []es.EventHandler
}

func (s *localHandler) HandleEvent(ctx context.Context, evt *es.Event) error {
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
