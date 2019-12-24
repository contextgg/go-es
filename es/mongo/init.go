package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	// AggregatesCollection for storing information regarding aggregates
	AggregatesCollection = "aggregates"
	// EventsCollection for storing events
	EventsCollection = "events"
)

// Create will setup a database
func Create(uri, db, username, password string) (*mongo.Database, error) {
	opts := options.
		Client().
		ApplyURI(uri)

	if len(username) > 0 {
		creds := options.Credential{
			Username: username,
			Password: password,
		}
		opts = opts.SetAuth(creds)
	}

	var err error
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	// test it!
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	options.CreateIndexes()

	database := client.
		Database(db)

	indexOpts := options.
		CreateIndexes().
		SetMaxTime(10 * time.Second)

	aggregatesIndex := mongo.IndexModel{
		Keys: bson.M{
			"aggregate_type": 1,
			"aggregate_id":   1,
		},
		Options: options.
			Index().
			SetUnique(true).
			SetName("aggreates.id.type"),
	}
	eventsIndex := mongo.IndexModel{
		Keys: bson.M{
			"aggregate_type": 1,
			"aggregate_id":   1,
			"version":        1,
		},
		Options: options.
			Index().
			SetUnique(true).
			SetName("events.id.type.version"),
	}

	if _, err := database.
		Collection(AggregatesCollection).
		Indexes().
		CreateOne(ctx, aggregatesIndex, indexOpts); err != nil {
		return nil, err
	}
	if _, err := database.
		Collection(EventsCollection).
		Indexes().
		CreateOne(ctx, eventsIndex, indexOpts); err != nil {
		return nil, err
	}

	return database, nil
}
