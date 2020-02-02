package mongo

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/contextgg/go-es/es"
)

var (
	// ErrVersionMismatch when the stored version doesn't match
	ErrVersionMismatch = errors.New("Aggregate version mismatch")
	// ErrStreamIDMismatch when the stream ids don't match
	ErrStreamIDMismatch = errors.New("StreamID is wrong")
)

// NewStore generates a new store to access to mongodb
func NewStore(db *mongo.Database, factory es.EventDataFactory) (es.DataStore, error) {
	return &store{db, factory}, nil
}

// Client for access to mongodb
type store struct {
	db      *mongo.Database
	factory es.EventDataFactory
}

// LoadStream will load from the stream
func (c *store) LoadStream(ctx context.Context, id string) (*es.Stream, error) {
	logger := log.
		With().
		Str("id", id).
		Logger()

	filter := bson.M{
		"id": id,
	}

	// load up the aggregate by ID!
	stream := &StreamDB{}
	if err := c.db.
		Collection(StreamsCollection).
		FindOne(ctx, filter).
		Decode(&stream); err != nil && err != mongo.ErrNoDocuments {
		logger.
			Error().
			Err(err).
			Msg("Could not load stream")
		return nil, err
	}

	return &es.Stream{
		ID:      stream.ID,
		Type:    stream.Type,
		Version: stream.Version,
	}, nil
}

// SaveStream will store the stream
func (c *store) SaveStream(ctx context.Context, stream *es.Stream) error {
	logger := log.
		With().
		Str("id", stream.ID).
		Str("type", stream.Type).
		Int("version", stream.Version).
		Logger()

	filter := bson.M{
		"id": stream.ID,
	}

	updateOptions := options.
		Update().
		SetUpsert(true)
	update := bson.M{
		"$set": bson.M{
			"id":      stream.ID,
			"type":    stream.Type,
			"version": stream.Version,
		},
	}

	if _, err := c.db.
		Collection(StreamsCollection).
		UpdateOne(ctx, filter, update, updateOptions); err != nil {
		logger.
			Error().
			Err(err).
			Msg("Could not insert stream")
		return err
	}

	return nil
}

// LoadMissingEvents for an aggregate
func (c *store) LoadMissingEvents(ctx context.Context, aggregate es.AggregateSourced) ([]*es.Event, error) {
	id := aggregate.GetID()
	fromVersion := aggregate.GetVersion()

	logger := log.
		With().
		Str("streamid", id).
		Int("fromVersion", fromVersion).
		Logger()

	events := []*es.Event{}
	query := bson.M{
		"streamid": id,
		"version":  bson.M{"$gt": fromVersion},
	}
	cur, err := c.db.
		Collection(EventsCollection).
		Find(ctx, query)
	if err != nil {
		logger.
			Error().
			Err(err).
			Msg("Couldn't find events")
		return nil, err
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var item EventDB
		if err := cur.Decode(&item); err != nil {
			return nil, err
		}

		logger.
			Debug().
			Interface("data", item.Data).
			Msg("Do we have raw data")

		// create the even
		data, err := c.factory(item.Type)
		if err != nil {
			logger.
				Error().
				Err(err).
				Str("type", item.Type).
				Msg("Issue creating the factory")
			return nil, err
		}

		if err := item.Data.Unmarshal(data); err != nil {
			logger.
				Error().
				Err(err).
				Str("type", item.Type).
				Msg("Issue unmarshalling")
			return nil, err
		}

		events = append(events, &es.Event{
			StreamID:  item.StreamID,
			Version:   item.Version,
			Type:      item.Type,
			Data:      data,
			Timestamp: item.Timestamp,
		})
	}

	logger.Debug().Interface("events", events).Msg("What are the events")
	return events, nil
}

// SaveEvents will store the unsaved events
func (c *store) SaveEvents(ctx context.Context, aggregate es.AggregateSourced, events []*es.Event) error {
	if len(events) == 0 {
		log.Debug().Msg("No events")
		return nil
	}

	streamID := aggregate.GetID()
	streamType := aggregate.GetTypeName()
	version := aggregate.GetVersion()

	logger := log.
		With().
		Str("streamID", streamID).
		Str("streamType", streamType).
		Int("version", version).
		Logger()

	maxVersion := version
	items := []interface{}{}
	for _, event := range events {
		if streamID != event.StreamID {
			return ErrStreamIDMismatch
		}

		var data *bson.RawValue
		if event.Data != nil {
			b, err := bson.Marshal(event.Data)
			if err != nil {
				return err
			}
			data = &bson.RawValue{
				Type:  bson.TypeEmbeddedDocument,
				Value: b,
			}
		}

		item := &EventDB{
			StreamID:  event.StreamID,
			Type:      event.Type,
			Version:   event.Version,
			Timestamp: event.Timestamp,
			Data:      data,
		}
		items = append(items, item)

		if maxVersion < event.Version {
			maxVersion = event.Version
		}
	}

	stream, err := c.LoadStream(ctx, streamID)
	if err != nil {
		logger.
			Error().
			Err(err).
			Msg("Error loading stream")
		return err
	}

	logger.Debug().Int("version", stream.Version).Msg("Got a version?")
	if stream.Version != version {
		logger.
			Error().
			Err(ErrVersionMismatch).
			Msg("Version issues")
		return ErrVersionMismatch
	}

	if err := c.SaveStream(ctx, es.NewStream(streamID, streamType, maxVersion)); err != nil {
		logger.
			Error().
			Err(err).
			Msg("Error saving stream")
		return err
	}

	// store all events
	if _, err := c.db.
		Collection(EventsCollection).
		InsertMany(ctx, items); err != nil {
		logger.
			Error().
			Err(err).
			Msg("Could not insert many events")
		return err
	}

	logger.
		Debug().
		Msg("Success")
	return nil
}

// LoadSnapshot will get a snapshot by aggregate
func (c *store) LoadSnapshot(ctx context.Context, revision int, aggregate es.AggregateSourced) (*es.Snapshot, error) {
	id := aggregate.GetID()
	version := aggregate.GetVersion()

	filter := bson.M{
		"streamid": id,
		"version":  version,
		"revision": revision,
	}

	logger := log.
		With().
		Str("streamid", id).
		Int("version", version).
		Int("revision", revision).
		Logger()

	// load up the aggregate by ID!
	snapshot := &SnapshotDB{}
	if err := c.db.
		Collection(SnapshotsCollection).
		FindOne(ctx, filter).
		Decode(&snapshot); err != nil && err != mongo.ErrNoDocuments {
		logger.
			Error().
			Err(err).
			Msg("Could not load snapshot")
		return nil, err
	}

	if snapshot.Data != nil {
		if err := snapshot.Data.Unmarshal(aggregate); err != nil {
			logger.
				Error().
				Err(err).
				Msg("Issue unmarshalling")
			return nil, err
		}
	}

	return es.NewSnapshot(id, version, revision, snapshot.Data), nil
}

// SaveSnapshot will get a snapshot by aggregate
func (c *store) SaveSnapshot(ctx context.Context, revision int, aggregate es.AggregateSourced) (*es.Snapshot, error) {
	id := aggregate.GetID()
	version := aggregate.GetVersion()

	filter := bson.M{
		"streamid": id,
		"version":  version,
		"revision": revision,
	}

	logger := log.
		With().
		Str("streamid", id).
		Int("version", version).
		Int("revision", revision).
		Logger()

	b, err := bson.Marshal(aggregate)
	if err != nil {
		return nil, err
	}
	data := &bson.RawValue{
		Type:  bson.TypeEmbeddedDocument,
		Value: b,
	}

	updateOptions := options.
		Update().
		SetUpsert(true)
	update := bson.M{
		"$set": bson.M{
			"streamid": id,
			"version":  version,
			"revision": revision,
			"data":     data,
		},
	}

	if _, err := c.db.
		Collection(SnapshotsCollection).
		UpdateOne(ctx, filter, update, updateOptions); err != nil {
		logger.
			Error().
			Err(err).
			Msg("Could not insert snapshot")
		return nil, err
	}

	return es.NewSnapshot(id, version, revision, data), nil
}

// LoadAggregate will get an aggregate by id
func (c *store) LoadAggregate(ctx context.Context, aggregate es.Aggregate) error {
	id := aggregate.GetID()
	typeName := aggregate.GetTypeName()

	query := bson.M{
		"id": id,
	}
	if err := c.db.
		Collection(typeName).
		FindOne(ctx, query).
		Decode(aggregate); err != nil && err != mongo.ErrNoDocuments {
		return err
	}

	return nil
}

// SaveAggregate create or update aggregate
func (c *store) SaveAggregate(ctx context.Context, aggregate es.Aggregate) error {
	id := aggregate.GetID()
	typeName := aggregate.GetTypeName()

	selector := bson.M{"id": id}
	update := bson.M{"$set": aggregate}

	opts := options.
		Update().
		SetUpsert(true)

	_, err := c.db.
		Collection(typeName).
		UpdateOne(ctx, selector, update, opts)

	return err
}

// Close underlying connection
func (c *store) Close() error {
	if c.db != nil {
		return c.db.
			Client().
			Disconnect(context.TODO())
	}
	return nil
}
