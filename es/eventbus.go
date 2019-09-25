package es

import "context"

// EventBus for publishing events
type EventBus interface {
	// PublishEvent publishes the event on the bus.
	PublishEvent(context.Context, *Event) error
	Close()
}

type combined struct {
	buses []EventBus
}

func (c *combined) PublishEvent(ctx context.Context, evt *Event) error {
	for _, b := range c.buses {
		if err := b.PublishEvent(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}
func (c *combined) Close() {
	for _, b := range c.buses {
		b.Close()
	}
}

// NewCombinedEventBus joined multiple together
func NewCombinedEventBus(buses ...EventBus) EventBus {
	return &combined{buses}
}
