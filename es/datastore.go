package es

import (
	"context"
)

// DataStore in charge of saving and loading events and aggregates from a data store
type DataStore interface {
	SaveStream(context.Context, *Stream) error
	LoadStream(context.Context, string) (*Stream, error)

	SaveEvents(context.Context, []*Event, int) error
	LoadEvents(context.Context, string, int) ([]*Event, error)

	SaveSnapshot(context.Context, Snapshot) error
	LoadSnapshot(context.Context) (*Snapshot, error)

	SaveAggregate(context.Context, Aggregate) error
	LoadAggregate(context.Context, Aggregate) error

	Close() error
}
