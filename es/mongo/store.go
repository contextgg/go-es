package mongo

import (
	"context"
	"errors"

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
	if len(events) < 1 {
		return nil
	}

	aggregateID := events[0].AggregateID
	aggregateType := events[0].AggregateType
	maxVersion := version

	items := []interface{}{}
	for _, event := range events {
		var raw []byte

		// Marshal the data like a good person!
		if event.Data != nil {
			var err error
			raw, err = bson.Marshal(event.Data)
			if err != nil {
				return err
			}
		}

		item := &EventDB{
			AggregateID:   event.AggregateID,
			AggregateType: event.AggregateType,
			Type:          event.Type,
			Version:       event.Version,
			Timestamp:     event.Timestamp,
			RawData:       raw,
		}
		items = append(items, item)

		if maxVersion < event.Version {
			maxVersion = event.Version
		}
	}

	if version == 0 {
		// store the aggregate so we can confirm it later!.
		aggregate := AggregateDB{AggregateID: aggregateID, AggregateType: aggregateType, Version: maxVersion}
		if _, err := c.db.
			Collection(AggregatesCollection).
			InsertOne(ctx, &aggregate); err != nil {
			return err
		}
	} else {
		// load up the aggregate by ID!
		var aggregate AggregateDB
		query := bson.M{
			"aggregate_id":   aggregateID,
			"aggregate_type": aggregateType,
		}
		if err := c.db.
			Collection(AggregatesCollection).
			FindOne(ctx, query).
			Decode(&aggregate); err != nil {
			return err
		}
		if aggregate.Version != version {
			return ErrVersionMismatch
		}

		if _, err := c.db.
			Collection(AggregatesCollection).
			UpdateOne(
				ctx,
				query,
				bson.M{
					"$inc": bson.M{"version": len(events)},
				}); err != nil {
			return err
		}
	}

	// store all events
	if _, err := c.db.
		Collection(EventsCollection).
		InsertMany(ctx, items); err != nil {
		return err
	}

	return nil
}

// Load the events from the data store
func (c *store) LoadEvents(ctx context.Context, id string, typeName string, fromVersion int) ([]*es.Event, error) {
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
		return nil, err
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var item EventDB
		if err := cur.Decode(&item); err != nil {
			return nil, err
		}

		// create the even
		data, err := c.factory(item.Type)
		if err != nil {
			return nil, err
		}

		if err := bson.Unmarshal(item.RawData, &data); err != nil {
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

	return events, nil
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
