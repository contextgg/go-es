package es

import (
	"context"

	"github.com/rs/zerolog/log"
)

// NewAggregateStore creates a new store for a specific aggregate
func NewAggregateStore(factory AggregateFactory, dataStore DataStore, bus EventBus) *AggregateStore {
	return &AggregateStore{
		factory:   factory,
		dataStore: dataStore,
		bus:       bus,
	}
}

// AggregateStore for loading and saving to the datastore
type AggregateStore struct {
	factory   AggregateFactory
	dataStore DataStore
	bus       EventBus
}

// LoadAggregate from the datastore
func (a *AggregateStore) LoadAggregate(ctx context.Context, id string) (Aggregate, error) {
	aggregate, err := a.factory(id)
	if err != nil {
		return nil, err
	}
	if err := a.dataStore.LoadAggregate(ctx, aggregate); err != nil {
		return nil, err
	}
	return aggregate, nil
}

// SaveAggregate and handle events if needed
func (a *AggregateStore) SaveAggregate(ctx context.Context, aggregate Aggregate) error {
	sublogger := log.With().
		Str("id", aggregate.GetID()).
		Str("type_name", aggregate.GetTypeName()).
		Logger()

	if err := a.dataStore.SaveAggregate(ctx, aggregate); err != nil {
		sublogger.
			Error().
			Err(err).
			Msg("Could not save aggregate")
		return err
	}

	// Publish events if supported by the aggregate.
	if holder, ok := aggregate.(EventHolder); ok && a.bus != nil {
		events := holder.EventsToPublish()
		holder.ClearEvents()

		sublogger.
			Debug().
			Int("event_count", len(events)).
			Msg("Aggregate is an EventHolder")

		for _, e := range events {
			subsublogger := sublogger.
				With().
				Str("event_type", e.Type).
				Logger()

			if err := a.bus.HandleEvent(ctx, e); err != nil {
				subsublogger.
					Error().
					Err(err).
					Msg("Error handling event with EventBus")
				return err
			}

			subsublogger.
				Debug().
				Msg("Event handled by EventBus")
		}
	}

	sublogger.
		Debug().
		Msg("SaveAggregate successful")
	return nil
}
