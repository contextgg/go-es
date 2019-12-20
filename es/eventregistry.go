package es

import (
	"fmt"
	"reflect"
	"sync"
)

// EventType information about an event
type EventType struct {
	reflect.Type

	IsLocal bool
}

// EventRegistry stores events so we can deserialize from datastores
type EventRegistry interface {
	Set(source interface{}, isLocal bool)
	Get(name string) (interface{}, error)
	IsLocal(name string) (bool, error)
}

// NewEventRegistry creates a new EventRegistry
func NewEventRegistry() EventRegistry {
	return &eventRegistry{
		registry: make(map[string]*EventType),
	}
}

type eventRegistry struct {
	sync.RWMutex
	registry map[string]*EventType
}

// Set a new type
func (e *eventRegistry) Set(source interface{}, isLocal bool) {
	e.Lock()
	defer e.Unlock()

	rawType, name := GetTypeName(source)
	e.registry[name] = &EventType{rawType, isLocal}
}

// Get a type based on its name
func (e *eventRegistry) Get(name string) (interface{}, error) {
	rawType, ok := e.registry[name]
	if !ok {
		return nil, fmt.Errorf("Cannot find %s in registry", name)
	}

	return reflect.New(rawType).Interface(), nil
}

// IsLocal the name
func (e *eventRegistry) IsLocal(name string) (bool, error) {
	rawType, ok := e.registry[name]
	if !ok {
		return false, fmt.Errorf("Cannot find %s in registry", name)
	}

	return rawType.IsLocal, nil
}
