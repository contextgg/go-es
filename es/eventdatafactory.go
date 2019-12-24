package es

// EventDataFactory creates a type by name
type EventDataFactory func(string) (interface{}, error)
