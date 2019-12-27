package es

// BaseAggregateSourced to make our commands smaller
type BaseAggregateSourced struct {
	ID       string `bson:"id"`
	TypeName string `bson:"type_name"`
	Version  int    `bson:"version"`

	events []*Event
}

// Initialize the aggregate with id and type
func (a *BaseAggregateSourced) Initialize(id string, typeName string) {
	a.ID = id
	a.TypeName = typeName
}

// GetID of the aggregate
func (a *BaseAggregateSourced) GetID() string {
	return a.ID
}

// GetTypeName of the aggregate
func (a *BaseAggregateSourced) GetTypeName() string {
	return a.TypeName
}

// StoreEvent will add the event to a list which will be persisted later
func (a *BaseAggregateSourced) StoreEvent(data interface{}) {
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
func (a *BaseAggregateSourced) GetVersion() int {
	return a.Version
}

// IncrementVersion increments the version of the aggregate. It should be
// called after an event has been successfully applied.
func (a *BaseAggregateSourced) IncrementVersion() {
	a.Version = a.Version + 1
}

// Events returns all uncommitted events that are not yet saved.
func (a *BaseAggregateSourced) Events() []*Event {
	return a.events
}

// ClearEvents clears all uncommitted events after saving.
func (a *BaseAggregateSourced) ClearEvents() {
	a.events = nil
}
