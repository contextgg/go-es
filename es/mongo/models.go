package mongo

import (
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

// StreamDB defines a stream to ensure we don't have race conditions
type StreamDB struct {
	ID      string `bson:"id"`
	Type    string `bson:"type"`
	Version int    `bson:"version"`
}

// EventDB defines the structure of the events to be stored
type EventDB struct {
	StreamID  string         `bson:"streamid"`
	Version   int            `bson:"version"`
	Type      string         `bson:"type"`
	Timestamp time.Time      `bson:"timestamp"`
	Data      *bson.RawValue `bson:"data,omitempty"`
}

// SnapshotDB defines the structure of the snapshot
type SnapshotDB struct {
	StreamID string         `bson:"streamid"`
	Version  int            `bson:"version"`
	Revision int            `bson:"revision"`
	Data     *bson.RawValue `bson:"data,omitempty"`
}
