package es

// Command will find its way to an aggregate
type Command interface {
	GetAggregateID() string
}

// BaseCommand to make it easier to get the ID
type BaseCommand struct {
	AggregateID string `json:"aggregate_id"`
}

// GetAggregateID return the aggregate id
func (c *BaseCommand) GetAggregateID() string {
	return c.AggregateID
}

// SetID so we can set the ID
func (c *BaseCommand) SetID(id string) {
	c.AggregateID = id
}

// SettableID so we can automatically set IDs in middlewares
type SettableID interface {
	SetID(string)
}
