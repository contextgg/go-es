package es

// AggregateFactory creates an aggregate
type AggregateFactory func(string) (Aggregate, error)
