package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewEventBus create boring event bus
func NewEventBus() es.EventBus {
	return &eventBus{}
}

type eventBus struct {
}

func (b *eventBus) PublishEvent(context.Context, *es.Event) error {
	return nil
}
