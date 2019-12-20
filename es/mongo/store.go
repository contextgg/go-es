package mongo

import (
	"context"
	"errors"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/contextgg/go-es/es"
)

var (
	// ErrVersionMismatch when the stored version doesn't match
	ErrVersionMismatch = errors.New("Aggregate version mismatch")
)

const (
	aggregatesCollection = "aggregates"
	eventsCollection     = "events"
)

// AggregateDB defines an aggregate to ensure we don't have race conditions
type AggregateDB struct {
	AggregateID   string `bson:"aggregate_id"`
	AggregateType string `bson:"aggregate_type"`
	Version       int    `bson:"version"`
}

//EventDB defines the structure of the events to be stored
type EventDB struct {
	AggregateID   string      `bson:"aggregate_id"`
	AggregateType string      `bson:"aggregate_type"`
	Type          string      `bson:"event_type"`
	Version       int         `bson:"version"`
	Timestamp     time.Time   `bson:"timestamp"`
	RawData       bson.Raw    `bson:"data,omitempty"`
	data          interface{} `bson:"-"`
}

//Client for access to mongodb
type Client struct {
	db             string
	session        *mgo.Session
	registry       es.EventRegistry
	minVersionDiff int
}

//NewClient generates a new client to access to mongodb
func NewClient(uri, db string, registry es.EventRegistry, minVersionDiff int) (es.EventStore, error) {
	session, err := mgo.Dial(uri)
	if err != nil {
		return nil, err
	}

	session.SetMode(mgo.Strong, true)
	session.SetSafe(&mgo.Safe{W: 1})

	cli := &Client{
		db,
		session,
		registry,
		minVersionDiff,
	}

	// Indexes
	aggregatesIndex := mgo.Index{
		Key:        []string{"aggregate_type", "aggregate_id"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
		Name:       "aggreates.id.type",
	}
	eventsIndex := mgo.Index{
		Key:        []string{"aggregate_type", "aggregate_id", "version"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     true,
		Name:       "events.id.type.version",
	}

	if err := session.DB(db).C(aggregatesCollection).EnsureIndex(aggregatesIndex); err != nil {
		return nil, err
	}

	if err := session.DB(db).C(eventsCollection).EnsureIndex(eventsIndex); err != nil {
		return nil, err
	}

	return cli, nil
}

// Save the events ensuring the current version
func (c *Client) SaveEvents(ctx context.Context, events []*es.Event, version int) error {
	if len(events) < 1 {
		return nil
	}

	sess := c.session.Copy()
	defer sess.Close()

	aggregateID := events[0].AggregateID
	aggregateType := events[0].AggregateType
	maxVersion := version

	items := []interface{}{}
	for _, event := range events {
		var raw bson.Raw

		// Marshal the data like a good person!
		if event.Data != nil {
			rawData, err := bson.Marshal(event.Data)
			if err != nil {
				return err
			}
			raw = bson.Raw{Kind: 3, Data: rawData}
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
		if err := sess.DB(c.db).C(aggregatesCollection).Insert(&aggregate); err != nil {
			return err
		}

	} else {
		// load up the aggregate by ID!
		var aggregate AggregateDB

		query := bson.M{
			"aggregate_id":   aggregateID,
			"aggregate_type": aggregateType,
		}
		if err := sess.DB(c.db).C(aggregatesCollection).Find(query).One(&aggregate); err != nil {
			return err
		}
		if aggregate.Version != version {
			return ErrVersionMismatch
		}

		if err := sess.DB(c.db).C(aggregatesCollection).Update(
			query,
			bson.M{
				"$inc": bson.M{"version": len(events)},
			}); err != nil {
			return err
		}
	}

	// serialize all the events!
	bulk := sess.DB(c.db).C(eventsCollection).Bulk()
	bulk.Insert(items...)
	if _, err := bulk.Run(); err != nil {
		return err
	}
	return nil
}

// Load the events from the data store
func (c *Client) LoadEvents(ctx context.Context, id string, typeName string, fromVersion int) ([]*es.Event, error) {
	sess := c.session.Copy()
	defer sess.Close()

	events := []*es.Event{}

	query := bson.M{
		"aggregate_id":   id,
		"aggregate_type": typeName,
		"version":        bson.M{"$gt": fromVersion},
	}
	iter := sess.DB(c.db).C(eventsCollection).Find(query).Iter()

	var item EventDB
	for iter.Next(&item) {
		if err := iter.Err(); err != nil {
			return nil, err
		}

		// create the even
		data, err := c.registry.Get(item.Type)
		if err != nil {
			return nil, err
		}

		if err := item.RawData.Unmarshal(data); err != nil {
			return nil, err
		}

		event := es.Event{
			Type:          item.Type,
			Timestamp:     item.Timestamp,
			AggregateID:   item.AggregateID,
			AggregateType: item.AggregateType,
			Version:       item.Version,
			Data:          data,
		}

		item.data = data
		item.RawData = bson.Raw{}

		events = append(events, &event)
	}

	return events, nil
}

// Save the events ensuring the current version
func (c *Client) SaveAggregate(ctx context.Context, previousVersion int, aggregate es.Aggregate) error {
	// when min is < 0 we have disabled snapshotting
	if c.minVersionDiff < 0 {
		return nil
	}

	diff := aggregate.GetVersion() - previousVersion

	// no need to snapshot yet
	if diff < c.minVersionDiff {
		return nil
	}

	sess := c.session.Copy()
	defer sess.Close()

	id := aggregate.GetID()
	typeName := aggregate.GetTypeName()

	selector := bson.M{"id": id}
	update := bson.M{"$set": aggregate}

	_, err := sess.
		DB(c.db).
		C(typeName).
		Upsert(selector, update)

	return err
}

// Load the events from the data store
func (c *Client) LoadAggregate(ctx context.Context, aggregate es.Aggregate) error {
	// when min is < 0 we have disabled snapshotting
	if c.minVersionDiff < 0 {
		return nil
	}

	sess := c.session.Copy()
	defer sess.Close()

	id := aggregate.GetID()
	typeName := aggregate.GetTypeName()

	query := bson.M{
		"id": id,
	}
	if err := sess.
		DB(c.db).
		C(typeName).
		Find(query).
		One(aggregate); err != nil && err != mgo.ErrNotFound {
		return err
	}

	return nil
}

// Close underlying connection
func (c *Client) Close() {
	if c.session != nil {
		c.session.Close()
	}
}
