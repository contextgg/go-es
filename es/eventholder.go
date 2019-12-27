package es

// EventHolder holds events that will be published
type EventHolder interface {
	// EventsToPublish returns all events to publish.
	EventsToPublish() []*Event
	// ClearEvents clears all events after a publish.
	ClearEvents()
}
