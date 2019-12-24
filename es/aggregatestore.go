package es

import "context"

// NewAggregateStore creates a new store for a specific aggregate
func NewAggregateStore(factory AggregateFactory, dataStore DataStore) *AggregateStore {
	return &AggregateStore{
		factory:   factory,
		dataStore: dataStore,
	}
}

// AggregateStore for loading and saving to the datastore
type AggregateStore struct {
	factory   AggregateFactory
	dataStore DataStore
}

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

func (a *AggregateStore) SaveAggregate(ctx context.Context, aggregate Aggregate) error {
	// TODO check the type?

	return a.dataStore.SaveAggregate(ctx, aggregate)
}
