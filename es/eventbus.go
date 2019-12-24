package es

import (
	"context"
)

// EventBus for creating commands
type EventBus interface {
	EventHandler
	Close()
}

// NewEventBus to handle aggregates
func NewEventBus(
	handler EventHandler,
	canPublish EventMatcher,
	publishers []EventPublisher,
) EventBus {
	return &eventBus{
		handler:    handler,
		canPublish: canPublish,
		publishers: publishers,
	}
}

type eventBus struct {
	handler    EventHandler
	canPublish EventMatcher
	publishers []EventPublisher
}

func (b *eventBus) HandleEvent(ctx context.Context, evt *Event) error {
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
