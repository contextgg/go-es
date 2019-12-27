package es

import (
	"context"
)

// Aggregate for replaying events against a single object
type Aggregate interface {
	// Initialize the aggregate with id and type
	Initialize(string, string)

	// ID return the ID of the aggregate
	GetID() string

	// GetTypeName return the TypeBame of the aggregate
	GetTypeName() string
}

// AggregateSourced for event stored aggregates
type AggregateSourced interface {
	Aggregate
	CommandHandler

	// StoreEvent will create an event and store it
	StoreEvent(interface{})

	// GetVersion returns the version of the aggregate.
	GetVersion() int

	// Increment version increments the version of the aggregate. It should be
	// called after an event has been successfully applied.
	IncrementVersion()

	// ApplyEvent applies an event on the aggregate by setting its values.
	// If there are no errors the version should be incremented by calling
	// IncrementVersion.
	ApplyEvent(context.Context, interface{}) error

	// Events returns all uncommitted events that are not yet saved.
	Events() []*Event

	// ClearEvents clears all uncommitted events after saving.
	ClearEvents()
}
