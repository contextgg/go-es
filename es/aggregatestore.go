package es

import "context"

// NewAggregateStore creates a new store for a specific aggregate
func NewAggregateStore(factory AggregateFactory, dataStore DataStore, bus EventBus) *AggregateStore {
	return &AggregateStore{
		factory:   factory,
		dataStore: dataStore,
		bus:       bus,
	}
}

// AggregateStore for loading and saving to the datastore
type AggregateStore struct {
	factory   AggregateFactory
	dataStore DataStore
	bus       EventBus
}

// LoadAggregate from the datastore
func (a *AggregateStore) LoadAggregate(ctx context.Context, id string) (Aggregate, error) {
	aggregate, err := a.factory(id)
	if err != nil {
		return nil, err
	}
	if err := a.dataStore.LoadAggregate(ctx, aggregate); err != nil {
		return nil, err
	}
	return aggregate, nil
}

// SaveAggregate and handle events if needed
func (a *AggregateStore) SaveAggregate(ctx context.Context, aggregate Aggregate) error {
	// TODO check the type?

	if err := a.dataStore.SaveAggregate(ctx, aggregate); err != nil {
		return err
	}

	// Publish events if supported by the aggregate.
	if holder, ok := aggregate.(EventHolder); ok && a.bus != nil {
		events := holder.EventsToPublish()
		holder.ClearEvents()

		for _, e := range events {
			if err := a.bus.HandleEvent(ctx, e); err != nil {
				return err
			}
		}
	}

	return nil
}
