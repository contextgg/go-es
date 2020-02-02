package es

import (
	"context"
)

// DataStore in charge of saving and loading events and aggregates from a data store
type DataStore interface {
	LoadStream(context.Context, string) (*Stream, error)
	SaveStream(context.Context, *Stream) error

	LoadMissingEvents(context.Context, AggregateSourced) ([]*Event, error)
	SaveEvents(context.Context, AggregateSourced, []*Event) error

	LoadSnapshot(context.Context, int, AggregateSourced) (*Snapshot, error)
	SaveSnapshot(context.Context, int, AggregateSourced) (*Snapshot, error)

	LoadAggregate(context.Context, Aggregate) error
	SaveAggregate(context.Context, Aggregate) error

	Close() error
}
