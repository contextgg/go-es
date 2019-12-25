package builder

import (
	"reflect"

	"github.com/contextgg/go-es/es"
	"github.com/contextgg/go-es/es/basic"
	"github.com/contextgg/go-es/es/mongo"
	"github.com/contextgg/go-es/es/nats"
)

// EventHandlerFactory builds an eventhandler
type EventHandlerFactory func(es.CommandBus) es.EventHandler

// CommandHandlerSetter builds an eventhandler
type CommandHandlerSetter func(es.CommandBus, es.DataStore, es.EventBus) error

// DataStoreFactory create an event store
type DataStoreFactory func(es.EventRegistry) (es.DataStore, error)

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
func LocalStore() DataStoreFactory {
	return func(es.EventRegistry) (es.DataStore, error) {
		return basic.NewMemoryStore(), nil
	}
}

// Mongo generates a MongoDB implementation of EventStore
func Mongo(uri, db, username, password string) DataStoreFactory {
	return func(r es.EventRegistry) (es.DataStore, error) {
		data, err := mongo.Create(uri, db, username, password)
		if err != nil {
			return nil, err
		}

		return mongo.NewStore(data, r.Get)
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
	GetDataStore() es.DataStore

	RegisterEvents(events ...*EventConfig)
	AddPublisher(publisher EventPublisherFactory)
	SetDefaultSnapshotMin(min int)

	WireSaga(saga es.Saga, events ...interface{})
	WireAggregate(aggregate *AggregateConfig, commands ...*CommandConfig)
	WireCommandHandler(handler es.CommandHandler, commands ...*CommandConfig)

	MakeAggregateStore(aggregate es.Aggregate) *es.AggregateStore

	Build() (*Client, error)
}

// NewClientBuilder create a new client builder
func NewClientBuilder(storeFactory DataStoreFactory) (ClientBuilder, error) {
	registry := es.NewEventRegistry()
	store, err := storeFactory(registry)
	if err != nil {
		return nil, err
	}

	return &builder{
		eventRegistry: registry,
		dataStore:     store,
		snapshotMin:   -1,
		eventBus:      es.NewEventBus(registry),
	}, nil
}

type builder struct {
	eventRegistry es.EventRegistry
	dataStore     es.DataStore
	eventBus      es.EventBus
	snapshotMin   int

	eventPublisherFactories []EventPublisherFactory
	eventHandlerFactories   []EventHandlerFactory
	commandHandlerSetters   []CommandHandlerSetter
}

func (b *builder) GetDataStore() es.DataStore {
	return b.dataStore
}

func (b *builder) RegisterEvents(events ...*EventConfig) {
	for _, evt := range events {
		b.eventRegistry.Set(evt.Event, evt.IsLocal)
	}
}

func (b *builder) AddPublisher(factory EventPublisherFactory) {
	b.eventPublisherFactories = append(b.eventPublisherFactories, factory)
}

func (b *builder) SetDefaultSnapshotMin(min int) {
	b.snapshotMin = min
}

func (b *builder) WireSaga(saga es.Saga, events ...interface{}) {
	var creater = func(b es.CommandBus) es.EventHandler {
		return es.NewSagaHandler(b, saga, es.MatchAnyEventOf(events))
	}

	// make the handler!
	b.eventHandlerFactories = append(b.eventHandlerFactories, creater)
}

func (b *builder) WireAggregate(aggregate *AggregateConfig, commands ...*CommandConfig) {
	t, name := es.GetTypeName(aggregate.Aggregate)

	var fn = func(commandBus es.CommandBus, store es.DataStore, eventBus es.EventBus) error {
		handler := es.NewAggregateHandler(t, name, store, eventBus, b.snapshotMin)
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
	var fn = func(commandBus es.CommandBus, store es.DataStore, eventBus es.EventBus) error {
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

func (b *builder) MakeAggregateStore(aggregate es.Aggregate) *es.AggregateStore {
	t, name := es.GetTypeName(aggregate)
	factory := func(id string) (es.Aggregate, error) {
		aggregate, ok := reflect.
			New(t).
			Interface().(es.Aggregate)
		if !ok {
			return nil, es.ErrCreatingAggregate
		}
		aggregate.Initialize(id, name)
		return aggregate, nil
	}

	return es.NewAggregateStore(factory, b.dataStore, b.eventBus)
}

func (b *builder) Build() (*Client, error) {
	commandBus := es.NewCommandBus()

	// create the event handlers
	for _, fn := range b.eventHandlerFactories {
		eh := fn(commandBus)
		b.eventBus.AddHandler(eh)
	}

	for _, fn := range b.eventPublisherFactories {
		p, err := fn()
		if err != nil {
			return nil, err
		}
		b.eventBus.AddPublisher(p)
	}

	for _, fn := range b.commandHandlerSetters {
		if err := fn(commandBus, b.dataStore, b.eventBus); err != nil {
			return nil, err
		}
	}

	return NewClient(b.dataStore, b.eventRegistry, b.eventBus, commandBus), nil
}
