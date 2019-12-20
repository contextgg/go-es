package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewEventBus to handle aggregates
func NewEventBus(
	registry es.EventRegistry,
	handlers []es.EventHandler,
	publishers []es.EventPublisher,
) es.EventBus {
	return &eventBus{
		registry:   registry,
		handlers:   handlers,
		publishers: publishers,
	}
}

type eventBus struct {
	registry   es.EventRegistry
	handlers   []es.EventHandler
	publishers []es.EventPublisher
}

func (b *eventBus) HandleEvent(ctx context.Context, evt *es.Event) error {
	// handle the events locally first.
	for _, h := range b.handlers {
		if err := h.HandleEvent(ctx, evt); err != nil {
			return err
		}
	}

	// look it up
	isLocal, err := b.registry.IsLocal(evt.Type)
	if err != nil {
		return err
	}

	if isLocal {
		return nil
	}

	for _, p := range b.publishers {
		if err := p.PublishEvent(ctx, evt); err != nil {
			return err
		}
	}

	return nil
}

// Close underlying connection
func (b *eventBus) Close() {
}
