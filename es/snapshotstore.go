package es

import "context"

// SnapshotStore in charge of saving and loading aggregates
type SnapshotStore interface {
	Save(context.Context, int, Aggregate) error
	Load(context.Context, Aggregate) error
	Close()
}
