package es

import "context"

// EventPublisher for publishing events
type EventPublisher interface {
	// PublishEvent the event on the bus.
	PublishEvent(context.Context, *Event) error
	Close()
}
