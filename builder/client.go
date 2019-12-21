package builder

import (
	"github.com/contextgg/go-es/es"
)

// NewClient creates a client
func NewClient(
	eventStore es.EventStore,
	eventRegistry es.EventRegistry,
	eventHandler es.EventHandler,
	eventBus es.EventBus,
	commandBus es.CommandBus,
) *Client {
	return &Client{
		EventStore:    eventStore,
		EventRegistry: eventRegistry,
		EventHandler:  eventHandler,
		EventBus:      eventBus,
		CommandBus:    commandBus,
	}
}

// Client has all the info / services for our ES platform
type Client struct {
	EventStore    es.EventStore
	EventRegistry es.EventRegistry
	EventHandler  es.EventHandler
	EventBus      es.EventBus
	CommandBus    es.CommandBus
}

// Close all the underlying services
func (c *Client) Close() {
	if c.EventBus != nil {
		c.EventBus.Close()
	}
	if c.EventStore != nil {
		c.EventStore.Close()
	}
}
