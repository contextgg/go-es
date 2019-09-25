package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewEventBus create boring event bus
func NewEventBus(handlers ...es.EventHandler) es.EventBus {
	return &eventBus{handlers}
}

type eventBus struct {
	handlers []es.EventHandler
}

func (b *eventBus) PublishEvent(ctx context.Context, evt *es.Event) error {
	for _, h := range b.handlers {
		if err := h.HandleEvent(ctx, evt); err != nil {
			return err
		}
	}

	return nil
}

// Close underlying connection
func (b *eventBus) Close() {
}
