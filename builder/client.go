package builder

import (
	"github.com/contextgg/go-es/es"
)

// Client has all the info / services for our ES platform
type Client struct {
	EventStore es.EventStore
	EventBus   es.EventBus
	CommandBus es.CommandBus
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
