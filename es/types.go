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

func NewStream(id string, sType string, version int) *Stream {
	return &Stream{
		ID:      id,
		Type:    sType,
		Version: version,
	}
}

type Snapshot struct {
	StreamID string
	Version  int
	Revision int
	Data     interface{}
}

func NewSnapshot(streamID string, version int, revision int, data interface{}) *Snapshot {
	return &Snapshot{
		StreamID: streamID,
		Version:  version,
		Revision: revision,
		Data:     data,
	}
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

func NewEventForAggregate(id string, version int, data interface{}) *Event {
	timestamp := GetTimestamp()
	_, typeName := GetTypeName(data)

	return &Event{
		StreamID:  id,
		Version:   version,
		Type:      typeName,
		Timestamp: timestamp,
		Data:      data,
	}
}
