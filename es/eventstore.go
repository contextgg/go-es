package es

import "context"

// EventStore in charge of saving and loading events from a data store
type EventStore interface {
	SaveEvents(context.Context, []*Event, int) error
	LoadEvents(context.Context, string, string, int) ([]*Event, error)
	SaveAggregate(context.Context, int, Aggregate) error
	LoadAggregate(context.Context, Aggregate) error
	Close()
}
