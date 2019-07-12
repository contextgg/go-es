package es

// Command will find its way to an aggregate
type Command interface {
	GetAggregateID() string
}

// BaseCommand to make it easier to get the ID
type BaseCommand struct {
	AggregateID string
}

// GetAggregateID return the aggregate id
func (c *BaseCommand) GetAggregateID() string {
	return c.AggregateID
}
