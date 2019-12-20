package es

import "context"

// Saga takes a events and may return new commands
type Saga interface {
	Run(context.Context, *Event) ([]Command, error)
}
