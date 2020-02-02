package mongo

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	// StreamsCollection for storing information regarding aggregates
	StreamsCollection = "streams"
	// EventsCollection for storing events
	EventsCollection = "events"
	// SnapshotsCollection for storing snapshots
	SnapshotsCollection = "snapshots"
)

// Create will setup a database
func Create(uri, db, username, password string, createIndexes bool) (*mongo.Database, error) {
	sublogger := log.With().
		Str("uri", uri).
		Str("db", db).
		Str("username", username).
		Bool("createIndexes", createIndexes).
		Logger()

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
		sublogger.
			Error().
			Err(err).
			Msg("Could not connect to db")
		return nil, err
	}

	// test it!
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		sublogger.
			Error().
			Err(err).
			Msg("Could not ping db")
		return nil, err
	}

	database := client.
		Database(db)

	if createIndexes {
		indexOpts := options.
			CreateIndexes().
			SetMaxTime(10 * time.Second)

		streamsIndex := mongo.IndexModel{
			Keys: bson.M{
				"id":   1,
				"type": 1,
			},
			Options: options.
				Index().
				SetUnique(true).
				SetName("streams.id.type"),
		}

		eventsIndex := mongo.IndexModel{
			Keys: bson.M{
				"streamid": 1,
				"type":     1,
				"version":  1,
			},
			Options: options.
				Index().
				SetUnique(true).
				SetName("events.streamid.type.version"),
		}

		snapshotsIndex := mongo.IndexModel{
			Keys: bson.M{
				"streamid": 1,
				"version":  1,
				"revision": 1,
			},
			Options: options.
				Index().
				SetUnique(true).
				SetName("snapshots.streamid.version.revision"),
		}

		database.
			Collection(StreamsCollection).
			Indexes().
			CreateOne(ctx, streamsIndex, indexOpts)
		database.
			Collection(EventsCollection).
			Indexes().
			CreateOne(ctx, eventsIndex, indexOpts)
		database.
			Collection(SnapshotsCollection).
			Indexes().
			CreateOne(ctx, snapshotsIndex, indexOpts)

		log.Debug().
			Msg("Indexes may have been created successfully")
	}

	log.Debug().
		Msg("Database created successfully")
	return database, nil
}
