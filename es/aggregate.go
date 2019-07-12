package es

import (
	"context"
)

// Aggregate for replaying events against a single object
type Aggregate interface {
	CommandHandler

	// Initialize the aggregate with id and type
	Initialize(string, string)

	// StoreEvent will create an event and store it
	StoreEvent(interface{})

	// Version returns the version of the aggregate.
	Version() int
	// Increment version increments the version of the aggregate. It should be
	// called after an event has been successfully applied.
	IncrementVersion()

	// Events returns all uncommitted events that are not yet saved.
	Events() []*Event
	// ClearEvents clears all uncommitted events after saving.
	ClearEvents()

	// ApplyEvent applies an event on the aggregate by setting its values.
	// If there are no errors the version should be incremented by calling
	// IncrementVersion.
	ApplyEvent(context.Context, interface{}) error
}

// NewBaseAggregate create new base aggregate
func NewBaseAggregate(id string) *BaseAggregate {
	return &BaseAggregate{
		id: id,
	}
}

// BaseAggregate to make our commands smaller
type BaseAggregate struct {
	id       string
	typeName string
	version  int
	events   []*Event
}

// Initialize the aggregate with id and type
func (a *BaseAggregate) Initialize(id string, typeName string) {
	a.id = id
	a.typeName = typeName
}

// StoreEvent will add the event to a list which will be persisted later
func (a *BaseAggregate) StoreEvent(data interface{}) {
	v := a.Version() + len(a.events) + 1
	timestamp := GetTimestamp()
	_, typeName := GetTypeName(data)
	e := &Event{
		Type:          typeName,
		Timestamp:     timestamp,
		AggregateID:   a.id,
		AggregateType: a.typeName,
		Version:       v,
		Data:          data,
	}

	a.events = append(a.events, e)
}

// Version returns the version of the aggregate.
func (a *BaseAggregate) Version() int {
	return a.version
}

// IncrementVersion increments the version of the aggregate. It should be
// called after an event has been successfully applied.
func (a *BaseAggregate) IncrementVersion() {
	a.version = a.version + 1
}

// Events returns all uncommitted events that are not yet saved.
func (a *BaseAggregate) Events() []*Event {
	return a.events
}

// ClearEvents clears all uncommitted events after saving.
func (a *BaseAggregate) ClearEvents() {
	a.events = nil
}
