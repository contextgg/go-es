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

// Save the events ensuring the current version
func (c *store) SaveEvents(ctx context.Context, events []*es.Event, version int) error {
	if len(events) == 0 {
		log.Debug().Msg("No events")
		return nil
	}

	aggregateID := events[0].AggregateID
	aggregateType := events[0].AggregateType
	filter := bson.M{
		"aggregate_id":   aggregateID,
		"aggregate_type": aggregateType,
	}

	logger := log.
		With().
		Str("aggregateID", aggregateID).
		Str("aggregateType", aggregateType).
		Int("version", version).
		Logger()

	maxVersion := version
	items := []interface{}{}
	for _, event := range events {
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
			AggregateID:   event.AggregateID,
			AggregateType: event.AggregateType,
			Type:          event.Type,
			Version:       event.Version,
			Timestamp:     event.Timestamp,
			Data:          data,
		}
		items = append(items, item)

		if maxVersion < event.Version {
			maxVersion = event.Version
		}
	}

	// load up the aggregate by ID!
	aggregate := &AggregateDB{}
	if err := c.db.
		Collection(AggregatesCollection).
		FindOne(ctx, filter).
		Decode(&aggregate); err != nil && err != mongo.ErrNoDocuments {
		logger.
			Error().
			Err(err).
			Msg("Could not load aggregate")
		return err
	}

	logger.Debug().Int("version", aggregate.Version).Msg("Got a version?")

	if aggregate.Version != version {
		logger.
			Error().
			Err(ErrVersionMismatch).
			Msg("Version issues")
		return ErrVersionMismatch
	}
	aggregate.Version = maxVersion

	updateOptions := options.
		Update().
		SetUpsert(true)
	update := bson.M{
		"$set": bson.M{
			"aggregate_id":   aggregateID,
			"aggregate_type": aggregateType,
			"version":        maxVersion,
		},
	}
	if _, err := c.db.
		Collection(AggregatesCollection).
		UpdateOne(ctx, filter, update, updateOptions); err != nil {
		logger.
			Error().
			Err(err).
			Msg("Could not insert aggregate")
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

// Load the events from the data store
func (c *store) LoadEvents(ctx context.Context, id string, typeName string, fromVersion int) ([]*es.Event, error) {
	logger := log.
		With().
		Str("aggregateID", id).
		Str("aggregateType", typeName).
		Int("fromVersion", fromVersion).
		Logger()

	events := []*es.Event{}
	query := bson.M{
		"aggregate_id":   id,
		"aggregate_type": typeName,
		"version":        bson.M{"$gt": fromVersion},
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

		logger.Debug().Interface("data", item.Data).Msg("Do we have raw data")

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
			Type:          item.Type,
			Timestamp:     item.Timestamp,
			AggregateID:   item.AggregateID,
			AggregateType: item.AggregateType,
			Version:       item.Version,
			Data:          data,
		})
	}

	logger.Debug().Interface("events", events).Msg("What are the events")
	return events, nil
}

// Save the events ensuring the current version
func (c *store) SaveSnapshot(ctx context.Context, revision string, aggregate es.Aggregate) error {
	aggregateID := aggregate.GetID()
	aggregateType := aggregate.GetTypeName()

	filter := bson.M{
		"aggregate_id":   aggregateID,
		"aggregate_type": aggregateType,
		"revision":       revision,
	}
	update := bson.M{"$set": aggregate}

	opts := options.
		Update().
		SetUpsert(true)

	_, err := c.db.
		Collection(SnapshotsCollection).
		UpdateOne(ctx, filter, update, opts)

	return err
}

// Load the events from the data store
func (c *store) LoadSnapshot(ctx context.Context, revision string, aggregate es.Aggregate) error {
	aggregateID := aggregate.GetID()
	aggregateType := aggregate.GetTypeName()

	filter := bson.M{
		"aggregate_id":   aggregateID,
		"aggregate_type": aggregateType,
		"revision":       revision,
	}

	if err := c.db.
		Collection(SnapshotsCollection).
		FindOne(ctx, filter).
		Decode(aggregate); err != nil && err != mongo.ErrNoDocuments {
		return err
	}

	return nil
}

// Save the events ensuring the current version
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

// Load the events from the data store
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

// Close underlying connection
func (c *store) Close() error {
	if c.db != nil {
		return c.db.
			Client().
			Disconnect(context.TODO())
	}
	return nil
}
