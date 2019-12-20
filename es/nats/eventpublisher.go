package nats

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/contextgg/go-es/es"

	nats "github.com/nats-io/nats.go"
)

// Client nats
type Client struct {
	namespace string
	conn      *nats.Conn
}

func retryConnect(uri string, max int) (*nats.Conn, error) {
	count := 0
	for {
		client, err := nats.Connect(uri, nats.Name("es-publisher"), nats.MaxReconnects(-1))
		if client != nil && err == nil {
			return client, nil
		}

		count = count + 1
		if count >= max {
			return nil, fmt.Errorf("Could not connect to server after %d retries", count)
		}

		log.Println("Wait for brokers to come up.. ", uri)
		time.Sleep(3 * time.Second)
	}

}

// NewClient returns the basic client to access to nats
func NewClient(uri string, namespace string) (es.EventPublisher, error) {
	conn, err := retryConnect(uri, 5)
	if err != nil {
		return nil, err
	}

	return &Client{
		namespace,
		conn,
	}, nil
}

// PublishEvent via nats
func (c *Client) PublishEvent(ctx context.Context, event *es.Event) error {
	ec, err := nats.NewEncodedConn(c.conn, nats.JSON_ENCODER)
	if err != nil {
		return err
	}

	subj := c.namespace + "." + event.AggregateType
	if err := ec.Publish(subj, event); err != nil {
		return err
	}

	return nil
}

// Close underlying connection
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}
