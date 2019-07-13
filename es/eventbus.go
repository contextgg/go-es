package es

import "context"

// EventBus for publishing events
type EventBus interface {
	// PublishEvent publishes the event on the bus.
	PublishEvent(context.Context, *Event) error
	Close()
}
