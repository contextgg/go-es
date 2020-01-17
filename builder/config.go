package builder

import "github.com/contextgg/go-es/es"

// AggregateConfig hold information regarding aggregate
type AggregateConfig struct {
	AggregateFunc es.AggregateSourcedFunc
	Middleware    []es.CommandHandlerMiddleware
}

// CommandConfig hold information regarding command
type CommandConfig struct {
	Command    es.Command
	Middleware []es.CommandHandlerMiddleware
}

// EventConfig hold information regarding command
type EventConfig struct {
	Event   interface{}
	IsLocal bool
}
