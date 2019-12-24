package es

import "context"

// AggregateStore in charge of saving and loading events and aggregates from a data store
type AggregateStore interface {
	SaveEvents(context.Context, []*Event, int) error
	LoadEvents(context.Context, string, string, int) ([]*Event, error)
	SaveAggregate(context.Context, int, Aggregate) error
	LoadAggregate(context.Context, Aggregate) error
	Close()
}
