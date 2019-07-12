package basic

import (
	"context"
	"errors"
	"reflect"

	"github.com/contextgg/go-es/es"
)

// ApplyEventError is when an event could not be applied. It contains the error
// and the event that caused it.
type ApplyEventError struct {
	// Event is the event that caused the error.
	Event *es.Event
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
)

// NewCommandHandler to handle aggregates
func NewCommandHandler(aggregateType reflect.Type, aggregateName string, store es.EventStore, bus es.EventBus) es.CommandHandler {
	return &handler{
		aggregateType: aggregateType,
		aggregateName: aggregateName,
		store:         store,
		bus:           bus,
	}
}

type handler struct {
	aggregateName string
	aggregateType reflect.Type
	registry      es.EventRegister
	store         es.EventStore
	bus           es.EventBus
}

func (h *handler) HandleCommand(ctx context.Context, cmd es.Command) error {
	id := cmd.GetAggregateID()

	aggregate := reflect.
		New(h.aggregateType).
		Interface().(es.Aggregate)
	aggregate.Initialize(id, h.aggregateName)

	// TODO load up the snapshots
	// if err := h.snapshot.Load(ctx, id, &aggregate); err != nil {
	// 	return err
	// }

	originalVersion := aggregate.Version()

	// load up the events from the DB.
	originalEvents, err := h.store.Load(ctx, id, h.aggregateName, originalVersion)
	if err != nil {
		return err
	}
	if err := h.applyEvents(ctx, aggregate, originalEvents); err != nil {
		return err
	}

	// handle the commands
	if err := aggregate.HandleCommand(ctx, cmd); err != nil {
		return err
	}

	// now save it!.
	events := aggregate.Events()
	if len(events) < 1 {
		return nil
	}
	if err := h.store.Save(ctx, events, aggregate.Version()); err != nil {
		return err
	}
	aggregate.ClearEvents()

	// Apply the events in case the aggregate needs to be further used
	// after this save. Currently it is not reused.
	if err := h.applyEvents(ctx, aggregate, events); err != nil {
		return err
	}

	// TODO save the snapshot!
	// if err := h.snapshot.Save(ctx, aggregate); err != nil {
	// 	return err
	// }

	for _, e := range events {
		if err := h.bus.PublishEvent(ctx, e); err != nil {
			return err
		}
	}

	return nil
}

func (h *handler) applyEvents(ctx context.Context, a es.Aggregate, events []*es.Event) error {
	for _, event := range events {
		if event.AggregateType != h.aggregateName {
			return ErrMismatchedEventType
		}

		// lets build the event!
		if err := a.ApplyEvent(ctx, event.Data); err != nil {
			return ApplyEventError{
				Event: event,
				Err:   err,
			}
		}
		a.IncrementVersion()
	}

	return nil
}
