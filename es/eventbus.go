package es

// EventBus for creating commands
type EventBus interface {
	EventHandler
	Close()
}
