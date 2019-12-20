package basic

import (
	"context"
	"fmt"

	"github.com/contextgg/go-es/es"
)

// NewEventStore create boring event store
func NewEventStore() es.EventStore {
	return &eventStore{
		all: make(map[string][]*es.Event),
	}
}

type eventStore struct {
	all map[string][]*es.Event
}

func (b *eventStore) SaveEvents(ctx context.Context, events []*es.Event, version int) error {
	if len(events) < 1 {
		return nil
	}

	id := events[0].AggregateID
	typeName := events[0].AggregateType

	index := fmt.Sprintf("%s.%s", typeName, id)

	// get the existing stuff!.
	existing := b.all[index]
	b.all[index] = append(existing, events...)
	return nil
}

func (b *eventStore) LoadEvents(ctx context.Context, id, typeName string, fromVersion int) ([]*es.Event, error) {
	index := fmt.Sprintf("%s.%s", typeName, id)

	existing := b.all[index]
	if existing == nil {
		return []*es.Event{}, nil
	}
	if fromVersion < 1 {
		return existing, nil
	}

	filteredEvents := []*es.Event{}
	for _, e := range existing {
		if e.Version > fromVersion {
			filteredEvents = append(filteredEvents, e)
		}
	}

	return filteredEvents, nil
}

func (b *eventStore) SaveAggregate(context.Context, int, es.Aggregate) error {
	return nil
}
func (b *eventStore) LoadAggregate(context.Context, es.Aggregate) error {
	return nil
}

// Close underlying connection
func (b *eventStore) Close() {
}
