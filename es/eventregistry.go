package es

import (
	"fmt"
	"reflect"
	"sync"
)

// EventRegister stores events so we can deserialize from datastores
type EventRegister interface {
	Set(source interface{})
	Get(name string) (interface{}, error)
}

// NewEventRegister creates a new EventRegister
func NewEventRegister() EventRegister {
	return &eventRegister{
		registry: make(map[string]reflect.Type),
	}
}

type eventRegister struct {
	sync.RWMutex
	registry map[string]reflect.Type
}

// Set a new type
func (e *eventRegister) Set(source interface{}) {
	e.Lock()
	defer e.Unlock()

	rawType, name := GetTypeName(source)
	e.registry[name] = rawType
}

// Get a type based on its name
func (e *eventRegister) Get(name string) (interface{}, error) {
	e.RLock()
	defer e.RUnlock()

	rawType, ok := e.registry[name]
	if !ok {
		return nil, fmt.Errorf("Cannot find %s in registry", name)
	}

	return reflect.New(rawType).Interface(), nil
}
