package builder

import (
	"github.com/contextgg/go-es/es"
)

// NewClient creates a client
func NewClient(
	aggregateStore es.AggregateStore,
	eventRegistry es.EventRegistry,
	eventHandler es.EventHandler,
	eventBus es.EventBus,
	commandBus es.CommandBus,
) *Client {
	return &Client{
		AggregateStore: aggregateStore,
		EventRegistry:  eventRegistry,
		EventHandler:   eventHandler,
		EventBus:       eventBus,
		CommandBus:     commandBus,
	}
}

// Client has all the info / services for our ES platform
type Client struct {
	AggregateStore es.AggregateStore
	EventRegistry  es.EventRegistry
	EventHandler   es.EventHandler
	EventBus       es.EventBus
	CommandBus     es.CommandBus
}

// Close all the underlying services
func (c *Client) Close() {
	if c.EventBus != nil {
		c.EventBus.Close()
	}
	if c.AggregateStore != nil {
		c.AggregateStore.Close()
	}
}
