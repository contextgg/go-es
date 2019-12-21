package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewEventBus to handle aggregates
func NewEventBus(
	handler es.EventHandler,
	canPublish es.EventMatcher,
	publishers []es.EventPublisher,
) es.EventBus {
	return &eventBus{
		handler:    handler,
		canPublish: canPublish,
		publishers: publishers,
	}
}

type eventBus struct {
	handler    es.EventHandler
	canPublish es.EventMatcher
	publishers []es.EventPublisher
}

func (b *eventBus) HandleEvent(ctx context.Context, evt *es.Event) error {
	if err := b.handler.HandleEvent(ctx, evt); err != nil {
		return err
	}

	if !b.canPublish(evt) {
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
	for _, p := range b.publishers {
		p.Close()
	}
}
