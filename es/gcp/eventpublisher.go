package gcp

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog/log"

	"github.com/contextgg/go-es/es"
)

// Client nats
type Client struct {
	client *pubsub.Client
	topic  *pubsub.Topic
}

// NewClient returns the basic client to access to nats
func NewClient(projectID string, topicName string) (es.EventPublisher, error) {

	// TODO: maybe we look into subscribing for events?
	ctx := context.Background()
	cli, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.
			Error().
			Err(err).
			Str("projectID", projectID).
			Str("topicName", topicName).
			Msg("pubsub.NewClient")
		return nil, fmt.Errorf("pubsub.NewClient: %v", err)
	}

	topic := cli.Topic(topicName)
	if ok, err := topic.Exists(ctx); err != nil {
		log.
			Error().
			Err(err).
			Msg("topic.Exists")
		return nil, err
	} else if !ok {
		if topic, err = cli.CreateTopic(ctx, topicName); err != nil {
			log.
				Error().
				Err(err).
				Str("topicName", topicName).
				Msg("cli.CreateTopic")
			return nil, err
		}
	}

	return &Client{
		client: cli,
		topic:  topic,
	}, nil
}

// PublishEvent via nats
func (c *Client) PublishEvent(ctx context.Context, event *es.Event) error {
	msg, err := json.Marshal(event)
	if err != nil {
		log.
			Error().
			Err(err).
			Msg("json.Marshal")
		return err
	}

	publishCtx := context.Background()
	res := c.topic.Publish(publishCtx, &pubsub.Message{
		Data: msg,
	})
	if _, err := res.Get(publishCtx); err != nil {
		log.
			Error().
			Err(err).
			Msg("Could not publish event")
		return err
	}

	log.
		Debug().
		Str("topic_id", c.topic.ID()).
		Str("event_type", event.Type).
		Str("event_aggregate_id", event.AggregateID).
		Str("event_aggregate_type", event.AggregateType).
		Msg("Event Published via GCP pub/sub")
	return nil
}

// Close underlying connection
func (c *Client) Close() {
	if c.client != nil {
		log.
			Debug().
			Msg("Closing the pubsub connection")
		c.client.Close()
	}
}
