package nats

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/contextgg/go-es/es"

	nats "github.com/nats-io/go-nats"
)

// Client nats
type Client struct {
	namespace string
	options   nats.Options
}

// NewClient returns the basic client to access to nats
func NewClient(urls string, useTLS bool, namespace string) (es.EventBus, error) {
	opts := nats.DefaultOptions
	opts.Secure = useTLS
	opts.Servers = strings.Split(urls, ",")

	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}

	return &Client{
		namespace,
		opts,
	}, nil
}

// PublishEvent via nats
func (c *Client) PublishEvent(ctx context.Context, event *es.Event) error {
	nc, err := c.options.Connect()
	if err != nil {
		return err
	}
	defer nc.Close()

	blob, err := json.Marshal(event)
	if err != nil {
		return err
	}

	subj := c.namespace + "." + event.AggregateType
	nc.Publish(subj, blob)
	nc.Flush()

	err = nc.LastError()
	return err
}
