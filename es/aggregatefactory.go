package es

import "reflect"

// AggregateFactory creates an aggregate
type AggregateFactory func(string) (Aggregate, error)

// AggregateSourcedFactory creates an aggregate
type AggregateSourcedFactory func(string) (AggregateSourced, error)

// NewAggregateFactory creates a factory from an aggregate
func NewAggregateFactory(aggregate Aggregate) AggregateFactory {
	t, name := GetTypeName(aggregate)

	return func(id string) (Aggregate, error) {
		aggregate, ok := reflect.
			New(t).
			Interface().(Aggregate)
		if !ok {
			return nil, ErrCreatingAggregate
		}
		aggregate.Initialize(id, name)
		return aggregate, nil
	}
}
