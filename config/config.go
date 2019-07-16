package config

import (
	"github.com/contextgg/go-es/es"
	"github.com/contextgg/go-es/es/basic"
	"github.com/contextgg/go-es/es/mongo"
	"github.com/contextgg/go-es/es/nats"
)

// CommandConfig should connect internally commands with an aggregate
type CommandConfig func(es.EventStore, es.EventBus, es.CommandRegister)

// EventBus returns an eventhus.EventBus impl
type EventBus func() (es.EventBus, error)

// EventStore returns an eventhus.EventStore impl
type EventStore func() (es.EventStore, error)

// Client has all the info / services for our ES platform
type Client struct {
	EventStore es.EventStore
	EventBus   es.EventBus
	CommandBus es.CommandBus
}

// Close all the underlying services
func (c *Client) Close() {
	if c.CommandBus != nil {
		c.CommandBus.Close()
	}
	if c.EventBus != nil {
		c.EventBus.Close()
	}
	if c.EventStore != nil {
		c.EventStore.Close()
	}
}

// NewClient will a client for our es pattern
func NewClient(storeFactory EventStore, eventBusFactory EventBus, commandConfigs ...CommandConfig) (*Client, error) {
	store, err := storeFactory()
	if err != nil {
		return nil, err
	}

	eventBus, err := eventBusFactory()
	if err != nil {
		return nil, err
	}

	registry := es.NewCommandRegister()
	for _, configs := range commandConfigs {
		configs(store, eventBus, registry)
	}

	client := &Client{
		EventBus:   eventBus,
		EventStore: store,
		CommandBus: basic.NewCommandBus(registry),
	}
	return client, nil
}

// WireAggregate will connect a list of commands to an aggregate
func WireAggregate(aggregate es.Aggregate, commands ...es.Command) CommandConfig {
	t, name := es.GetTypeName(aggregate)
	return func(store es.EventStore, eventBus es.EventBus, registry es.CommandRegister) {
		handler := basic.NewCommandHandler(t, name, store, eventBus)

		for _, cmd := range commands {
			registry.Add(cmd, handler)
		}
	}
}

// WireCommand for creating a basic command handler
func WireCommand(command es.Command, handler es.CommandHandler) CommandConfig {
	return func(store es.EventStore, eventBus es.EventBus, registry es.CommandRegister) {
		registry.Add(command, handler)
	}
}

// LocalStore used for testing
func LocalStore() EventStore {
	return func() (es.EventStore, error) {
		return basic.NewEventStore(), nil
	}
}

// LocalPublisher used for testing
func LocalPublisher() EventBus {
	return func() (es.EventBus, error) {
		return basic.NewEventBus(), nil
	}
}

// Nats generates a Nats implementation of EventBus
func Nats(uri string, namespace string) EventBus {
	return func() (es.EventBus, error) {
		return nats.NewClient(uri, namespace)
	}
}

// Mongo generates a MongoDB implementation of EventStore
func Mongo(uri, db string, events ...interface{}) EventStore {
	registry := es.NewEventRegister()
	for _, evt := range events {
		registry.Set(evt)
	}

	return func() (es.EventStore, error) {
		return mongo.NewClient(uri, db, registry)
	}
}
