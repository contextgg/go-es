package mongo

import (
	"time"
)

// AggregateDB defines an aggregate to ensure we don't have race conditions
type AggregateDB struct {
	AggregateID   string `bson:"aggregate_id"`
	AggregateType string `bson:"aggregate_type"`
	Version       int    `bson:"version"`
}

//EventDB defines the structure of the events to be stored
type EventDB struct {
	AggregateID   string    `bson:"aggregate_id"`
	AggregateType string    `bson:"aggregate_type"`
	Type          string    `bson:"event_type"`
	Version       int       `bson:"version"`
	Timestamp     time.Time `bson:"timestamp"`
	RawData       []byte    `bson:"data,omitempty"`
}
