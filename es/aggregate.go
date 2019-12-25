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

// NewBaseAggregate create new base aggregate
func NewBaseAggregate(id string) *BaseAggregate {
	return &BaseAggregate{
		ID: id,
	}
}

// BaseAggregate to make our commands smaller
type BaseAggregate struct {
	ID       string `bson:"id"`
	TypeName string `bson:"type_name"`
	Version  int    `bson:"version"`

	events []*Event
}

// Initialize the aggregate with id and type
func (a *BaseAggregate) Initialize(id string, typeName string) {
	a.ID = id
	a.TypeName = typeName
}

// GetID of the aggregate
func (a *BaseAggregate) GetID() string {
	return a.ID
}

// GetTypeName of the aggregate
func (a *BaseAggregate) GetTypeName() string {
	return a.TypeName
}

// StoreEvent will add the event to a list which will be persisted later
func (a *BaseAggregate) StoreEvent(data interface{}) {
	v := a.GetVersion() + len(a.events) + 1
	timestamp := GetTimestamp()
	_, typeName := GetTypeName(data)
	e := &Event{
		Type:          typeName,
		Timestamp:     timestamp,
		AggregateID:   a.ID,
		AggregateType: a.TypeName,
		Version:       v,
		Data:          data,
	}

	a.events = append(a.events, e)
}

// GetVersion returns the version of the aggregate.
func (a *BaseAggregate) GetVersion() int {
	return a.Version
}

// IncrementVersion increments the version of the aggregate. It should be
// called after an event has been successfully applied.
func (a *BaseAggregate) IncrementVersion() {
	a.Version = a.Version + 1
}

// Events returns all uncommitted events that are not yet saved.
func (a *BaseAggregate) Events() []*Event {
	return a.events
}

// ClearEvents clears all uncommitted events after saving.
func (a *BaseAggregate) ClearEvents() {
	a.events = nil
}
