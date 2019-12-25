package es

// EventHolder holds events that will be published
type EventHolder interface {
	// EventsToPublish returns all events to publish.
	EventsToPublish() []*Event
	// ClearEvents clears all events after a publish.
	ClearEvents()
}

// SliceEventHolder is an EventHolder using a slice to store events.
type SliceEventHolder []*Event

// PublishEvent registers an event to be published after the aggregate
// has been successfully saved.
func (a *SliceEventHolder) PublishEvent(e *Event) {
	*a = append(*a, e)
}

// EventsToPublish implements the EventsToPublish method of the EventPublisher interface.
func (a *SliceEventHolder) EventsToPublish() []*Event {
	return *a
}

// ClearEvents implements the ClearEvents method of the EventPublisher interface.
func (a *SliceEventHolder) ClearEvents() {
	*a = nil
}
