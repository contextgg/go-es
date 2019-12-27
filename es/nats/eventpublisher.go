package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/contextgg/go-es/es"

	nats "github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// Client nats
type Client struct {
	namespace string
	conn      *nats.EncodedConn
}

func natsLogger(msg string) nats.ConnHandler {
	return func(conn *nats.Conn) {
		log.
			Debug().
			Str("connected_url", conn.ConnectedUrl()).
			Msg(msg)
	}
}

func retryConnect(uri string, max int) (*nats.Conn, error) {
	log.
		Debug().
		Str("uri", uri).
		Msg("Try connecting to Nats")

	count := 0
	for count < max {
		if count > 0 {
			log.
				Debug().
				Str("uri", uri).
				Msg("Sleep while we wait for Nats server")
			time.Sleep(3 * time.Second)
		}

		client, err := nats.Connect(uri,
			nats.Name("es-publisher"),
			nats.MaxReconnects(-1),
			nats.ReconnectHandler(natsLogger("Nats reconnect handler")),
			nats.DisconnectHandler(natsLogger("Nats disconnect handler")),
		)
		if client != nil && err == nil {
			return client, nil
		}
		count = count + 1
	}

	return nil, fmt.Errorf("Could not connect to server after %d retries", count)
}

// NewClient returns the basic client to access to nats
func NewClient(uri string, namespace string) (es.EventPublisher, error) {
	conn, err := retryConnect(uri, 5)
	if err != nil {
		log.
			Error().
			Err(err).
			Str("uri", uri).
			Str("namespace", namespace).
			Msg("Could not setup Nats client")
		return nil, err
	}

	// TODO: maybe we look into subscribing for events?

	// setup the encoded connection here
	ec, err := nats.NewEncodedConn(conn, nats.JSON_ENCODER)
	if err != nil {
		log.
			Error().
			Err(err).
			Str("uri", uri).
			Str("namespace", namespace).
			Msg("Could not create encoded connection")
		return nil, err
	}

	return &Client{
		namespace,
		ec,
	}, nil
}

// PublishEvent via nats
func (c *Client) PublishEvent(ctx context.Context, event *es.Event) error {
	subj := c.namespace + "." + event.Type
	if err := c.conn.Publish(subj, event); err != nil {
		log.
			Error().
			Err(err).
			Str("subj", subj).
			Msg("Could not publish event")
		return err
	}

	log.
		Debug().
		Str("subj", subj).
		Str("event_type", event.Type).
		Str("event_aggregate_id", event.AggregateID).
		Str("event_aggregate_type", event.AggregateType).
		Msg("Event Published via Nats")
	return nil
}

// Close underlying connection
func (c *Client) Close() {
	if c.conn != nil {
		log.
			Debug().
			Msg("Closing the Nats connection")
		c.conn.Close()
	}
}
