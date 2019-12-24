package es

import "context"

type AggregateStore struct {
	dataStore DataStore
	factory   AggregateFactory
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
