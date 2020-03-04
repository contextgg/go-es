package es

import (
	"context"
)

// DataStore in charge of saving and loading events and aggregates from a data store
type DataStore interface {
	SaveEvents(context.Context, []*Event, int) error
	LoadEvents(context.Context, string, string, int) ([]*Event, error)
	SaveSnapshot(context.Context, string, Aggregate) error
	LoadSnapshot(context.Context, string, Aggregate) error
	SaveAggregate(context.Context, Aggregate) error
	LoadAggregate(context.Context, Aggregate) error
	Close() error
}
