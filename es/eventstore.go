package es

import "context"

// EventStore in charge of saving and loading events from a data store
type EventStore interface {
	Save(context.Context, []*Event, int) error
	Load(context.Context, string, string, int) ([]*Event, error)
}
