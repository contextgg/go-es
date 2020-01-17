package es

import "reflect"

// AggregateFactory creates an aggregate
type AggregateFactory func(string) (Aggregate, error)

// AggregateSourcedFactory creates an aggregate
type AggregateSourcedFactory func(string) (AggregateSourced, error)

// AggregateSourcedFunc will return a sourced
type AggregateSourcedFunc func() AggregateSourced

// NewAggregateSourcedFunc build a func that returns a new aggregate
func NewAggregateSourcedFunc(aggregate Aggregate) AggregateSourcedFunc {
	aggregateType, _ := GetTypeName(aggregate)

	return func() AggregateSourced {
		aggregate, ok := reflect.
			New(aggregateType).
			Interface().(AggregateSourced)
		if !ok {
			panic(ErrCreatingAggregate)
		}
		return aggregate
	}
}

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

// NewAggregateSourcedFactory builds an aggregate
func NewAggregateSourcedFactory(fn AggregateSourcedFunc) AggregateSourcedFactory {
	_, aggregateName := GetTypeName(fn())

	return func(id string) (AggregateSourced, error) {
		aggregate := fn()
		aggregate.Initialize(id, aggregateName)
		return aggregate, nil
	}
}
