package es

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

type TestDataStore struct {
}

func (d *TestDataStore) SaveEvents(context.Context, []*Event, int) error {
	return nil
}
func (d *TestDataStore) LoadEvents(context.Context, string, string, int) ([]*Event, error) {
	return nil, nil
}
func (d *TestDataStore) SaveSnapshot(context.Context, string, Aggregate) error {
	return nil
}
func (d *TestDataStore) LoadSnapshot(context.Context, string, Aggregate) error {
	return nil
}
func (d *TestDataStore) SaveAggregate(context.Context, Aggregate) error {
	return nil
}
func (d *TestDataStore) LoadAggregate(context.Context, Aggregate) error {
	return nil
}
func (d *TestDataStore) Close() error {
	return nil
}

type TestBus struct {
	Count int
}

func (t *TestBus) HandleEvent(context.Context, *Event) error {
	t.Count = t.Count + 1
	return nil
}
func (t *TestBus) AddPublisher(EventPublisher) {}
func (t *TestBus) Close()                      {}

// TestAggregate for testing our save
type TestAggregate struct {
	BaseAggregateHolder
}

// EventTested test event
type EventTested struct {
	Msg string
}

func isHolder(aggregate interface{}) (EventHolder, bool) {
	eh, ok := aggregate.(EventHolder)
	return eh, ok
}
func isAggregate(aggregate interface{}) (Aggregate, bool) {
	eh, ok := aggregate.(Aggregate)
	return eh, ok
}

func TestSavingAggregate(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	dataStore := &TestDataStore{}
	bus := &TestBus{}
	store := NewAggregateStore(nil, dataStore, bus)

	aggregate := &TestAggregate{}

	if _, ok := isHolder(aggregate); !ok {
		t.Error("This should be an event holder")
		return
	}
	if _, ok := isAggregate(aggregate); !ok {
		t.Error("This should be an aggregate")
		return
	}

	evt := NewEvent(&EventTested{"Hello"})
	aggregate.PublishEvent(evt)

	err := store.SaveAggregate(context.TODO(), aggregate)
	if err != nil {
		t.Error(err)
		return
	}

	if bus.Count != 1 {
		t.Error("Bus didn't handle the event")
	}
}
