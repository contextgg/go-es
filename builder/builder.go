package builder

import (
	"github.com/contextgg/go-es/es"
	"github.com/contextgg/go-es/es/basic"
	"github.com/contextgg/go-es/es/mongo"
	"github.com/contextgg/go-es/es/nats"
)

// EventHandlerFactory builds an eventhandler
type EventHandlerFactory func(es.CommandBus) es.EventHandler

// CommandHandlerSetter builds an eventhandler
type CommandHandlerSetter func(es.CommandBus, es.AggregateStore, es.EventBus) error

// AggregateStoreFactory create an event store
type AggregateStoreFactory func(es.EventRegistry) (es.AggregateStore, error)

// EventPublisherFactory create an event publisher
type EventPublisherFactory func(es.EventHandler) (es.EventPublisher, error)

// Aggregate creates a new AggregateConfig
func Aggregate(aggregate es.Aggregate, middleware ...es.CommandHandlerMiddleware) *AggregateConfig {
	return &AggregateConfig{
		Aggregate:  aggregate,
		Middleware: middleware,
	}
}

// Command creates a new CommandConfig
func Command(command es.Command, middleware ...es.CommandHandlerMiddleware) *CommandConfig {
	return &CommandConfig{
		Command:    command,
		Middleware: middleware,
	}
}

// Event creates a new EventConfig
func Event(source interface{}, islocal bool) *EventConfig {
	return &EventConfig{
		Event:   source,
		IsLocal: islocal,
	}
}

// LocalStore used for testing
func LocalStore() AggregateStoreFactory {
	return func(es.EventRegistry) (es.AggregateStore, error) {
		return basic.NewEventStore(), nil
	}
}

// Mongo generates a MongoDB implementation of EventStore
func Mongo(uri, db string, minVersionDiff int) AggregateStoreFactory {
	return func(r es.EventRegistry) (es.AggregateStore, error) {
		return mongo.NewClient(uri, db, r, minVersionDiff)
	}
}

// Nats generates a Nats implementation of EventBus
func Nats(uri string, namespace string) EventPublisherFactory {
	return func(handler es.EventHandler) (es.EventPublisher, error) {
		return nats.NewClient(uri, namespace, handler)
	}
}

// ClientBuilder for building a client we'll use
type ClientBuilder interface {
	GetAggregateStore() es.AggregateStore

	RegisterEvents(events ...*EventConfig)
	AddPublisher(publisher EventPublisherFactory)

	WireSaga(saga es.Saga, events ...interface{})
	WireAggregate(aggregate *AggregateConfig, commands ...*CommandConfig)
	WireCommandHandler(handler es.CommandHandler, commands ...*CommandConfig)

	Build() (*Client, error)
}

// NewClientBuilder create a new client builder
func NewClientBuilder(storeFactory AggregateStoreFactory) (ClientBuilder, error) {
	registry := es.NewEventRegistry()
	store, err := storeFactory(registry)
	if err != nil {
		return nil, err
	}

	return &builder{
		eventRegistry:  registry,
		aggregateStore: store,
	}, nil
}

type builder struct {
	eventRegistry  es.EventRegistry
	aggregateStore es.AggregateStore

	eventPublisherFactories []EventPublisherFactory
	eventHandlerFactories   []EventHandlerFactory
	commandHandlerSetters   []CommandHandlerSetter
}

func (b *builder) GetAggregateStore() es.AggregateStore {
	return b.aggregateStore
}

func (b *builder) RegisterEvents(events ...*EventConfig) {
	for _, evt := range events {
		b.eventRegistry.Set(evt.Event, evt.IsLocal)
	}
}

func (b *builder) AddPublisher(factory EventPublisherFactory) {
	b.eventPublisherFactories = append(b.eventPublisherFactories, factory)
}

func (b *builder) WireSaga(saga es.Saga, events ...interface{}) {
	var creater = func(b es.CommandBus) es.EventHandler {
		return basic.NewSagaHandler(b, saga, es.MatchAnyEventOf(events))
	}

	// make the handler!
	b.eventHandlerFactories = append(b.eventHandlerFactories, creater)
}
func (b *builder) WireAggregate(aggregate *AggregateConfig, commands ...*CommandConfig) {
	t, name := es.GetTypeName(aggregate.Aggregate)

	var fn = func(commandBus es.CommandBus, store es.AggregateStore, eventBus es.EventBus) error {
		handler := basic.NewCommandHandler(t, name, store, eventBus)
		handler = es.UseCommandHandlerMiddleware(handler, aggregate.Middleware...)

		for _, cmd := range commands {
			h := es.UseCommandHandlerMiddleware(handler, cmd.Middleware...)

			if err := commandBus.SetHandler(h, cmd.Command); err != nil {
				return err
			}
		}
		return nil
	}

	b.commandHandlerSetters = append(b.commandHandlerSetters, fn)
}
func (b *builder) WireCommandHandler(handler es.CommandHandler, commands ...*CommandConfig) {
	var fn = func(commandBus es.CommandBus, store es.AggregateStore, eventBus es.EventBus) error {
		for _, cmd := range commands {
			h := es.UseCommandHandlerMiddleware(handler, cmd.Middleware...)

			if err := commandBus.SetHandler(h, cmd.Command); err != nil {
				return err
			}
		}
		return nil
	}

	b.commandHandlerSetters = append(b.commandHandlerSetters, fn)
}
func (b *builder) Build() (*Client, error) {
	commandBus := basic.NewCommandBus()

	// create the event handlers
	eventHandlers := make([]es.EventHandler, len(b.eventHandlerFactories))
	for i, fn := range b.eventHandlerFactories {
		eventHandlers[i] = fn(commandBus)
	}

	// for handling local events
	eventHandler := basic.NewLocalHandler(b.eventRegistry, eventHandlers)

	eventPublishers := make([]es.EventPublisher, len(b.eventPublisherFactories))
	for i, fn := range b.eventPublisherFactories {
		p, err := fn(eventHandler)
		if err != nil {
			return nil, err
		}
		eventPublishers[i] = p
	}

	// create the event bus
	canPublish := es.MatchNotLocal(b.eventRegistry)
	eventBus := basic.NewEventBus(eventHandler, canPublish, eventPublishers)

	for _, fn := range b.commandHandlerSetters {
		if err := fn(commandBus, b.aggregateStore, eventBus); err != nil {
			return nil, err
		}
	}

	return NewClient(b.aggregateStore, b.eventRegistry, eventHandler, eventBus, commandBus), nil
}
