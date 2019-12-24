package es

import (
	"context"
	"errors"
	"reflect"
)

// ApplyEventError is when an event could not be applied. It contains the error
// and the event that caused it.
type ApplyEventError struct {
	// Event is the event that caused the error.
	Event *Event
	// Err is the error that happened when applying the event.
	Err error
}

// Error implements the Error method of the error interface.
func (a ApplyEventError) Error() string {
	return "failed to apply event " + a.Event.String() + ": " + a.Err.Error()
}

var (
	// ErrInvalidAggregateType is when the aggregate does not implement event.Aggregte.
	ErrInvalidAggregateType = errors.New("Invalid aggregate type")
	// ErrMismatchedEventType occurs when loaded events from ID does not match aggregate type.
	ErrMismatchedEventType = errors.New("mismatched event type and aggregate type")
	// ErrWrongVersion when the version number is wrong
	ErrWrongVersion = errors.New("When we compute the wrong version")
	// ErrCreatingAggregate whoops when creating aggregate
	ErrCreatingAggregate = errors.New("Issue create aggregate")
)

// NewAggregateHandler to handle aggregates
func NewAggregateHandler(
	aggregateType reflect.Type,
	aggregateName string,
	dataStore DataStore,
	eventBus EventBus,
	minVersionDiff int,
) CommandHandler {
	factory := func(id string) (Aggregate, error) {
		aggregate, ok := reflect.
			New(aggregateType).
			Interface().(Aggregate)
		if !ok {
			return nil, ErrCreatingAggregate
		}
		aggregate.Initialize(id, aggregateName)
		return aggregate, nil
	}

	return &aggregateHandler{
		factory:        factory,
		dataStore:      dataStore,
		eventBus:       eventBus,
		minVersionDiff: minVersionDiff,
	}
}

type aggregateHandler struct {
	factory        AggregateFactory
	dataStore      DataStore
	eventBus       EventBus
	minVersionDiff int
}

func (h *aggregateHandler) applyEvents(ctx context.Context, aggregate Aggregate, originalEvents []*Event) error {
	aggregateType := aggregate.GetTypeName()

	for _, event := range originalEvents {
		if event.AggregateType != aggregateType {
			return ErrMismatchedEventType
		}

		// lets build the event!
		if err := aggregate.ApplyEvent(ctx, event.Data); err != nil {
			return ApplyEventError{
				Event: event,
				Err:   err,
			}
		}
		aggregate.IncrementVersion()
	}
	return nil
}

func (h *aggregateHandler) HandleCommand(ctx context.Context, cmd Command) error {
	id := cmd.GetAggregateID()

	// make the aggregate
	aggregate, err := h.factory(id)
	if err != nil {
		return err
	}

	// load up the aggregate
	if h.minVersionDiff >= 0 {
		if err := h.dataStore.LoadAggregate(ctx, aggregate); err != nil {
			return err
		}
	}

	originalVersion := aggregate.GetVersion()
	aggregateType := aggregate.GetTypeName()

	// load up the events from the DB.
	originalEvents, err := h.dataStore.LoadEvents(ctx, id, aggregateType, originalVersion)
	if err != nil {
		return err
	}
	if err := h.applyEvents(ctx, aggregate, originalEvents); err != nil {
		return err
	}

	// handle the command
	if err := aggregate.HandleCommand(ctx, cmd); err != nil {
		return err
	}

	// now save it!.
	events := aggregate.Events()
	if len(events) > 0 {
		if err := h.dataStore.SaveEvents(ctx, events, aggregate.GetVersion()); err != nil {
			return err
		}
		aggregate.ClearEvents()

		// Apply the events so we can save the aggregate
		if err := h.applyEvents(ctx, aggregate, events); err != nil {
			return err
		}
	}

	// save the snapshot!
	diff := aggregate.GetVersion() - originalVersion
	if diff < 0 {
		return ErrWrongVersion
	}
	if diff > h.minVersionDiff {
		if err := h.dataStore.SaveAggregate(ctx, aggregate); err != nil {
			return err
		}
	}

	for _, e := range events {
		if err := h.eventBus.HandleEvent(ctx, e); err != nil {
			return err
		}
	}

	return nil
}
