package es

import (
	"fmt"
	"time"
)

type Event struct {
	StreamID  string
	Version   int
	Data      interface{}
	Type      string
	Metadata  interface{}
	Timestamp time.Time
}

// String implements the String method of the Event interface.
func (e Event) String() string {
	return fmt.Sprintf("%s@%d", e.Type, e.Version)
}

type Stream struct {
	ID      string
	Type    string
	Version int
}

type Snapshot struct {
	StreamID string
	Version  int
	Data     interface{}
	Revision int
}

// NewEvent will create an event from data
func NewEvent(data interface{}) *Event {
	timestamp := GetTimestamp()
	_, typeName := GetTypeName(data)
	return &Event{
		Type:      typeName,
		Timestamp: timestamp,
		Data:      data,
	}
}
