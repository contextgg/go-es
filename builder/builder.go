package builder

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/contextgg/go-es/es"
	"github.com/contextgg/go-es/es/basic"
	"github.com/contextgg/go-es/es/gcp"
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
	fn := es.NewAggregateSourcedFunc(aggregate)
	return AggregateFunc(fn, middleware...)
}

// AggregateFunc creates a new AggregateConfig
func AggregateFunc(fn es.AggregateSourcedFunc, middleware ...es.CommandHandlerMiddleware) *AggregateConfig {
	return &AggregateConfig{
		AggregateFunc: fn,
		Middleware:    middleware,
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
func Mongo(uri, db, username, password string, createIndexes bool) DataStoreFactory {
	return func(r es.EventRegistry) (es.DataStore, error) {
		data, err := mongo.Create(uri, db, username, password, createIndexes)
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

// GCPPubSub generates a pubsub implementation of EventBus
func GCPPubSub(projectID string, topicName string) EventPublisherFactory {
	return func() (es.EventPublisher, error) {
		return gcp.NewClient(projectID, topicName)
	}
}

// ClientBuilder for building a client we'll use
type ClientBuilder interface {
	GetDataStore() es.DataStore

	RegisterEvents(events ...*EventConfig)
	AddPublisher(publisher EventPublisherFactory)
	SetDefaultSnapshotMin(min int)
	SetDebug()

	WireSaga(saga es.Saga, events ...interface{})
	WireAggregate(aggregate *AggregateConfig, commands ...*CommandConfig)
	WireCommandHandler(handler es.CommandHandler, commands ...*CommandConfig)

	MakeAggregateStore(aggregate es.Aggregate) *es.AggregateStore

	Build() (*Client, error)
}

// NewClientBuilder create a new client builder
func NewClientBuilder(storeFactory DataStoreFactory) (ClientBuilder, error) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	registry := es.NewEventRegistry()
	store, err := storeFactory(registry)
	if err != nil {
		return nil, err
	}

	local := es.NewLocalEventHandler(registry)

	return &builder{
		eventRegistry: registry,
		dataStore:     store,
		snapshotMin:   -1,
		eventHandler:  local,
		eventBus:      es.NewEventBus(registry, local),
	}, nil
}

type builder struct {
	eventRegistry es.EventRegistry
	dataStore     es.DataStore
	eventBus      es.EventBus
	eventHandler  *es.LocalEventHandler
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

func (b *builder) SetDebug() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

func (b *builder) WireSaga(saga es.Saga, events ...interface{}) {
	var creater = func(b es.CommandBus) es.EventHandler {
		return es.NewSagaHandler(b, saga, es.MatchAnyEventOf(events...))
	}

	// make the handler!
	b.eventHandlerFactories = append(b.eventHandlerFactories, creater)
}

func (b *builder) WireAggregate(aggregate *AggregateConfig, commands ...*CommandConfig) {
	factory := es.NewAggregateSourcedFactory(aggregate.AggregateFunc)

	var fn = func(commandBus es.CommandBus, store es.DataStore, eventBus es.EventBus) error {
		handler := es.NewAggregateHandler(factory, store, eventBus, b.snapshotMin)
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
	factory := es.NewAggregateFactory(aggregate)
	return es.NewAggregateStore(factory, b.dataStore, b.eventBus)
}

func (b *builder) Build() (*Client, error) {
	log.Debug().Msg("Starting to build the go-es Client")

	commandBus := es.NewCommandBus()

	// create the event handlers
	for _, fn := range b.eventHandlerFactories {
		eh := fn(commandBus)
		b.eventHandler.AddHandler(eh)

		log.Debug().Msg("Event Handler added")
	}

	for _, fn := range b.eventPublisherFactories {
		p, err := fn()
		if err != nil {
			return nil, err
		}
		b.eventBus.AddPublisher(p)

		log.Debug().Msg("Event Publisher added")
	}

	for _, fn := range b.commandHandlerSetters {
		if err := fn(commandBus, b.dataStore, b.eventBus); err != nil {
			return nil, err
		}

		log.Debug().Msg("Command Handler configured")
	}

	return NewClient(b.dataStore, b.eventRegistry, b.eventHandler, b.eventBus, commandBus), nil
}
