package es

// AggregateFactory creates an aggregate
type AggregateFactory func(string) (Aggregate, error)

// AggregateSourcedFactory creates an aggregate
type AggregateSourcedFactory func(string) (AggregateSourced, error)
