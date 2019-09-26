package config

import (
	"github.com/contextgg/go-es/es"
	"github.com/contextgg/go-es/es/basic"
	"github.com/contextgg/go-es/es/mongo"
	"github.com/contextgg/go-es/es/nats"
)

// CommandConfig should connect internally commands with an aggregate
type CommandConfig func(es.EventStore, es.SnapshotStore, es.EventBus, es.CommandRegister)

// EventBus returns an es.EventBus impl
type EventBus func() (es.EventBus, error)

// EventStore returns an es.EventStore impl
type EventStore func() (es.EventStore, error)

// SnapshotStore returns an es.SnapshotStore impl
type SnapshotStore func() (es.SnapshotStore, error)

// AggregateConfig hold information regarding aggregate
type AggregateConfig struct {
	Aggregate  es.Aggregate
	Middleware []es.CommandHandlerMiddleware
}

// AggregateCommandConfig hold information regarding command
type AggregateCommandConfig struct {
	Command    es.Command
	Middleware []es.CommandHandlerMiddleware
}

// Client has all the info / services for our ES platform
type Client struct {
	EventStore    es.EventStore
	SnapshotStore es.SnapshotStore
	EventBus      es.EventBus
	CommandBus    es.CommandBus
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
	if c.SnapshotStore != nil {
		c.SnapshotStore.Close()
	}
}

// NewClient will a client for our es pattern
func NewClient(storeFactory EventStore, snapshotFactory SnapshotStore, eventBusFactory EventBus, commandConfigs ...CommandConfig) (*Client, error) {
	store, err := storeFactory()
	if err != nil {
		return nil, err
	}

	snapshot, err := snapshotFactory()
	if err != nil {
		return nil, err
	}

	eventBus, err := eventBusFactory()
	if err != nil {
		return nil, err
	}

	registry := es.NewCommandRegister()
	for _, configs := range commandConfigs {
		configs(store, snapshot, eventBus, registry)
	}

	client := &Client{
		EventBus:   eventBus,
		EventStore: store,
		CommandBus: basic.NewCommandBus(registry),
	}
	return client, nil
}

// Aggregate creates a new AggregateConfig
func Aggregate(aggregate es.Aggregate, middleware ...es.CommandHandlerMiddleware) *AggregateConfig {
	return &AggregateConfig{
		Aggregate:  aggregate,
		Middleware: middleware,
	}
}

// Command creates a new AggregateConfig
func Command(command es.Command, middleware ...es.CommandHandlerMiddleware) *AggregateCommandConfig {
	return &AggregateCommandConfig{
		Command:    command,
		Middleware: middleware,
	}
}

// WireAggregate will connect a list of commands to an aggregate
func WireAggregate(aggregate *AggregateConfig, commands ...*AggregateCommandConfig) CommandConfig {
	t, name := es.GetTypeName(aggregate.Aggregate)

	return func(store es.EventStore, snapshot es.SnapshotStore, eventBus es.EventBus, registry es.CommandRegister) {
		handler := basic.NewCommandHandler(t, name, store, snapshot, eventBus)
		handler = es.UseCommandHandlerMiddleware(handler, aggregate.Middleware...)

		for _, cmd := range commands {
			registry.Add(cmd.Command, es.UseCommandHandlerMiddleware(handler, cmd.Middleware...))
		}
	}
}

// WireCommand for creating a basic command handler
func WireCommand(command es.Command, handler es.CommandHandler) CommandConfig {
	return func(store es.EventStore, snapshot es.SnapshotStore, eventBus es.EventBus, registry es.CommandRegister) {
		registry.Add(command, handler)
	}
}

// LocalStore used for testing
func LocalStore() EventStore {
	return func() (es.EventStore, error) {
		return basic.NewEventStore(), nil
	}
}

// LocalSnapshot used for testing
func LocalSnapshot() SnapshotStore {
	return func() (es.SnapshotStore, error) {
		return basic.NewSnapshotStore(), nil
	}
}

// LocalPublisher used for testing
func LocalPublisher(handlers ...es.EventHandler) EventBus {
	return func() (es.EventBus, error) {
		return basic.NewEventBus(handlers...), nil
	}
}

// Nats generates a Nats implementation of EventBus
func Nats(uri string, namespace string) EventBus {
	return func() (es.EventBus, error) {
		return nats.NewClient(uri, namespace)
	}
}

// CombinedPublisher for multiple publishers
func CombinedPublisher(buses ...EventBus) EventBus {
	return func() (es.EventBus, error) {
		all := []es.EventBus{}

		for _, bus := range buses {
			b, err := bus()
			if err != nil {
				return nil, err
			}

			all = append(all, b)
		}

		return es.NewCombinedEventBus(all...), nil
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

// MongoSnapshot generates a MongoDB implementation of SnapshotStore
func MongoSnapshot(uri, db string, minDiff int) SnapshotStore {
	return func() (es.SnapshotStore, error) {
		return mongo.NewSnapshotClient(uri, db, minDiff)
	}
}
