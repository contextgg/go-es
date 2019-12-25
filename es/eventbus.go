package es

import (
	"context"
)

// EventBus for creating commands
type EventBus interface {
	EventHandler
	AddPublisher(EventPublisher)
	Close()
}

// NewEventBus to handle aggregates
func NewEventBus(
	registry EventRegistry,
	handler EventHandler,
) EventBus {
	return &eventBus{
		registry: registry,
		handler:  handler,
	}
}

type eventBus struct {
	registry   EventRegistry
	handler    EventHandler
	publishers []EventPublisher
}

func (b *eventBus) AddPublisher(publisher EventPublisher) {
	b.publishers = append(b.publishers, publisher)
}

func (b *eventBus) HandleEvent(ctx context.Context, evt *Event) error {
	if err := b.handler.HandleEvent(ctx, evt); err != nil {
		return err
	}

	matcher := MatchNotLocal(b.registry)
	if !matcher(evt) {
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
