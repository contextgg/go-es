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
		allAggregates: make(map[string]es.Aggregate),
	}

	for _, opt := range opts {
		opt(ms)
	}

	return ms
}

type memoryStore struct {
	allEvents     map[string][]*es.Event
	allAggregates map[string]es.Aggregate
}

func (b *memoryStore) LoadStream(context.Context, string) (*es.Stream, error) {
	return nil, nil
}
func (b *memoryStore) SaveStream(context.Context, *es.Stream) error {
	return nil
}
func (b *memoryStore) LoadMissingEvents(ctx context.Context, agg es.AggregateSourced) ([]*es.Event, error) {
	id := agg.GetID()
	typeName := agg.GetTypeName()
	fromVersion := agg.GetVersion()

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
func (b *memoryStore) SaveEvents(ctx context.Context, aggregate es.AggregateSourced, events []*es.Event) error {
	if len(events) < 1 {
		return nil
	}

	id := aggregate.GetID()
	typeName := aggregate.GetTypeName()

	index := fmt.Sprintf("%s.%s", typeName, id)

	// get the existing stuff!.
	existing := b.allEvents[index]
	b.allEvents[index] = append(existing, events...)
	return nil
}
func (b *memoryStore) LoadSnapshot(ctx context.Context, revision int, agg es.AggregateSourced) (*es.Snapshot, error) {
	return nil, nil
}
func (b *memoryStore) SaveSnapshot(ctx context.Context, revision int, agg es.AggregateSourced) (*es.Snapshot, error) {
	return nil, nil
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
