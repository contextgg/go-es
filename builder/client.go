package builder

import (
	"github.com/contextgg/go-es/es"
)

// NewClient creates a client
func NewClient(
	dataStore es.DataStore,
	eventRegistry es.EventRegistry,
	eventHandler es.EventHandler,
	eventBus es.EventBus,
	commandBus es.CommandBus,
) *Client {
	return &Client{
		DataStore:     dataStore,
		EventRegistry: eventRegistry,
		EventHandler:  eventHandler,
		EventBus:      eventBus,
		CommandBus:    commandBus,
	}
}

// Client has all the info / services for our ES platform
type Client struct {
	DataStore     es.DataStore
	EventRegistry es.EventRegistry
	EventHandler  es.EventHandler
	EventBus      es.EventBus
	CommandBus    es.CommandBus
}

// Close all the underlying services
func (c *Client) Close() error {
	if c.EventBus != nil {
		c.EventBus.Close()
	}
	if c.DataStore != nil {
		c.DataStore.Close()
	}
	return nil
}
