package es

import (
	"context"
)

// EventBus for creating commands
type EventBus interface {
	EventHandler
	AddHandler(EventHandler)
	AddPublisher(EventPublisher)
	Close()
}

// NewEventBus to handle aggregates
func NewEventBus(
	registry EventRegistry,
) EventBus {
	return &eventBus{
		registry: registry,
	}
}

type eventBus struct {
	registry   EventRegistry
	handlers   []EventHandler
	publishers []EventPublisher
}

func (b *eventBus) AddHandler(handler EventHandler) {
	b.handlers = append(b.handlers, handler)
}

func (b *eventBus) AddPublisher(publisher EventPublisher) {
	b.publishers = append(b.publishers, publisher)
}

func (b *eventBus) HandleEvent(ctx context.Context, evt *Event) error {
	handler := NewLocalEventHandler(b.registry, b.handlers)
	if err := handler.HandleEvent(ctx, evt); err != nil {
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
