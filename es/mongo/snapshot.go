package mongo

import (
	"context"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/contextgg/go-es/es"
)

// SnapshotClient for access to mongodb
type SnapshotClient struct {
	db      string
	session *mgo.Session

	min int
}

// NewSnapshotClient generates a new client to access to mongodb
func NewSnapshotClient(uri, db string, minVersionDiff int) (es.SnapshotStore, error) {
	session, err := mgo.Dial(uri)
	if err != nil {
		return nil, err
	}

	session.SetMode(mgo.Monotonic, true)
	//session.SetSafe(&mgo.Safe{W: 1})

	cli := &SnapshotClient{
		db,
		session,
		minVersionDiff,
	}

	return cli, nil
}

// Save the events ensuring the current version
func (c *SnapshotClient) Save(ctx context.Context, previousVersion int, aggregate es.Aggregate) error {
	// when min is < 0 we have disabled snapshotting
	if c.min < 0 {
		return nil
	}

	diff := aggregate.GetVersion() - previousVersion

	// no need to snapshot yet
	if diff < c.min {
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
func (c *SnapshotClient) Load(ctx context.Context, aggregate es.Aggregate) error {
	// when min is < 0 we have disabled snapshotting
	if c.min < 0 {
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
func (c *SnapshotClient) Close() {
	if c.session != nil {
		c.session.Close()
	}
}
