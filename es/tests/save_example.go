package tests

import (
	"context"
	"testing"

	"github.com/contextgg/go-es/builder"
	"github.com/contextgg/go-es/es"

	"github.com/stretchr/testify/assert"
)

type Event1 struct {
	Example string
}
type Event2 struct {
	Name string
}

func newEvent(id, aggType string, version int, data interface{}) *es.Event {
	timestamp := es.GetTimestamp()
	_, typeName := es.GetTypeName(data)
	return &es.Event{
		AggregateID:   id,
		AggregateType: aggType,
		Type:          typeName,
		Timestamp:     timestamp,
		Version:       version,
		Data:          data,
	}
}

func TestStorage(t *testing.T) {
	ef := es.NewEventRegistry()
	ef.Set(&Event1{}, false)
	ef.Set(&Event2{}, false)

	df := builder.Mongo("mongodb://localhost:27017", "test", "", "", true)

	store, err := df(ef)

	assert.NoError(t, err, "No db")
	assert.NotNil(t, store, "No store")

	data1 := &Event1{
		Example: "e1",
	}
	data2 := &Event2{
		Name: "e2",
	}
	evts := []*es.Event{
		newEvent("1", "test", 1, data1),
		newEvent("1", "test", 2, data2),
	}

	errEvents := store.SaveEvents(context.Background(), evts, 0)
	assert.NoError(t, errEvents, "Couldn't save")
}

func TestLoad(t *testing.T) {
	ef := es.NewEventRegistry()
	ef.Set(&Event1{}, false)
	ef.Set(&Event2{}, false)

	df := builder.Mongo("mongodb://localhost:27017", "test", "", "", true)
	store, err := df(ef)
	assert.NoError(t, err, "No db")
	assert.NotNil(t, store, "No store")

	evts, err := store.LoadEvents(context.Background(), "1", "test", 0)
	assert.NoError(t, err, "Couldn't load")
	assert.Equal(t, 2, len(evts), "Events?")
}
