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
type CommandHandlerSetter func(es.CommandBus, es.EventStore, es.EventBus) error

// EventStoreFactory create an event store
type EventStoreFactory func(es.EventRegistry) (es.EventStore, error)

// EventPublisherFactory create an event publisher
type EventPublisherFactory func() (es.EventPublisher, error)

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
func LocalStore() EventStoreFactory {
	return func(es.EventRegistry) (es.EventStore, error) {
		return basic.NewEventStore(), nil
	}
}

// Mongo generates a MongoDB implementation of EventStore
func Mongo(uri, db string, minVersionDiff int) EventStoreFactory {
	return func(r es.EventRegistry) (es.EventStore, error) {
		return mongo.NewClient(uri, db, r, minVersionDiff)
	}
}

// Nats generates a Nats implementation of EventBus
func Nats(uri string, namespace string) EventPublisherFactory {
	return func() (es.EventPublisher, error) {
		return nats.NewClient(uri, namespace)
	}
}

// ClientBuilder for building a client we'll use
type ClientBuilder interface {
	RegisterEvent(events ...*EventConfig)

	AddPublisher(publisher EventPublisherFactory)
	SetEventStore(factory EventStoreFactory)

	WireSaga(saga es.Saga, events ...interface{})
	WireAggregate(aggregate *AggregateConfig, commands ...*CommandConfig)
	WireCommandHandler(handler es.CommandHandler, commands ...*CommandConfig)

	Build() (*Client, error)
}

// NewClientBuilder create a new client builder
func NewClientBuilder() ClientBuilder {
	return &builder{
		eventStoreFactory: LocalStore(),
	}
}

type builder struct {
	events            []*EventConfig
	eventStoreFactory EventStoreFactory

	eventPublisherFactories []EventPublisherFactory
	eventHandlerFactories   []EventHandlerFactory
	commandHandlerSetters   []CommandHandlerSetter
}

func (b *builder) RegisterEvent(events ...*EventConfig) {
	b.events = events
}

func (b *builder) AddPublisher(factory EventPublisherFactory) {
	b.eventPublisherFactories = append(b.eventPublisherFactories, factory)
}
func (b *builder) SetEventStore(factory EventStoreFactory) {
	b.eventStoreFactory = factory
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

	var fn = func(commandBus es.CommandBus, store es.EventStore, eventBus es.EventBus) error {
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
	var fn = func(commandBus es.CommandBus, store es.EventStore, eventBus es.EventBus) error {
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

	// create the event register.
	eventRegistry := es.NewEventRegistry()
	for _, evt := range b.events {
		eventRegistry.Set(evt.Event, evt.IsLocal)
	}

	// create the event store
	eventStore, err := b.eventStoreFactory(eventRegistry)
	if err != nil {
		return nil, err
	}

	// create the event handlers
	eventHandlers := make([]es.EventHandler, len(b.eventHandlerFactories))
	for i, fn := range b.eventHandlerFactories {
		eventHandlers[i] = fn(commandBus)
	}

	eventPublishers := make([]es.EventPublisher, len(b.eventPublisherFactories))
	for i, fn := range b.eventPublisherFactories {
		p, err := fn()
		if err != nil {
			return nil, err
		}
		eventPublishers[i] = p
	}

	// create the event bus
	eventBus := basic.NewEventBus(eventRegistry, eventHandlers, eventPublishers)

	for _, fn := range b.commandHandlerSetters {
		if err := fn(commandBus, eventStore, eventBus); err != nil {
			return nil, err
		}
	}

	return &Client{
		EventStore: eventStore,
		EventBus:   eventBus,
		CommandBus: commandBus,
	}, nil
}
