package basic

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/contextgg/go-es/es"
)

// ErrAggregateNil guard our function
var ErrAggregateNil = errors.New("Aggregate is nil")

// Option so we can inject test data
type Option = func(*memoryStore)

// AddAggregate will add aggregate to the base
func AddAggregate(agg es.Aggregate) Option {
	return func(ms *memoryStore) {
		id := agg.GetID()
		ms.allAggregates[id] = agg
	}
}

// NewMemoryStore create boring event store
func NewMemoryStore(opts ...Option) es.DataStore {
	ms := &memoryStore{
		allEvents:     make(map[string][]*es.Event),
		allSnapshots:  make(map[string]es.Aggregate),
		allAggregates: make(map[string]es.Aggregate),
	}

	for _, opt := range opts {
		opt(ms)
	}

	return ms
}

type memoryStore struct {
	allEvents     map[string][]*es.Event
	allSnapshots  map[string]es.Aggregate
	allAggregates map[string]es.Aggregate
}

func (b *memoryStore) SaveEvents(ctx context.Context, events []*es.Event, version int) error {
	if len(events) < 1 {
		return nil
	}

	id := events[0].AggregateID
	typeName := events[0].AggregateType

	index := fmt.Sprintf("%s.%s", typeName, id)

	// get the existing stuff!.
	existing := b.allEvents[index]
	b.allEvents[index] = append(existing, events...)
	return nil
}

func (b *memoryStore) LoadEvents(ctx context.Context, id, typeName string, fromVersion int) ([]*es.Event, error) {
	index := fmt.Sprintf("%s.%s", typeName, id)

	existing := b.allEvents[index]
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

func (b *memoryStore) SaveSnapshot(ctx context.Context, revision string, agg es.Aggregate) error {
	if agg == nil {
		return ErrAggregateNil
	}

	id := agg.GetID() + "_" + revision
	b.allSnapshots[id] = agg
	return nil
}
func (b *memoryStore) LoadSnapshot(ctx context.Context, revision string, agg es.Aggregate) error {
	if agg == nil {
		return ErrAggregateNil
	}

	id := agg.GetID() + "_" + revision
	if nagg, ok := b.allSnapshots[id]; ok {
		set(agg, nagg)
	}
	return nil
}
func (b *memoryStore) SaveAggregate(ctx context.Context, agg es.Aggregate) error {
	if agg == nil {
		return ErrAggregateNil
	}

	id := agg.GetID()
	b.allAggregates[id] = agg
	return nil
}
func (b *memoryStore) LoadAggregate(ctx context.Context, agg es.Aggregate) error {
	if agg == nil {
		return ErrAggregateNil
	}

	id := agg.GetID()
	if nagg, ok := b.allAggregates[id]; ok {
		set(agg, nagg)
	}
	return nil
}

// Close underlying connection
func (b *memoryStore) Close() error {
	return nil
}

func set(x, y interface{}) {
	val := reflect.ValueOf(y).Elem()
	reflect.ValueOf(x).Elem().Set(val)
}
