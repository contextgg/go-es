package es

import "context"

// EventHandler for handling commands
type EventHandler interface {
	HandleEvent(context.Context, *Event) error
}

// EventHandlerFunc is a function that can be used as a event handler.
type EventHandlerFunc func(context.Context, *Event) error

// HandleEvent implements the HandleEvent method of the EventHandler.
func (h EventHandlerFunc) HandleEvent(ctx context.Context, e *Event) error {
	return h(ctx, e)
}

// EventHandlerMiddleware is a function that middlewares can implement to be
// able to chain.
type EventHandlerMiddleware func(EventHandler) EventHandler

// UseEventHandlerMiddleware wraps a EventHandler in one or more middleware.
func UseEventHandlerMiddleware(h EventHandler, middleware ...EventHandlerMiddleware) EventHandler {
	// Apply in reverse order.
	for i := len(middleware) - 1; i >= 0; i-- {
		m := middleware[i]
		h = m(h)
	}
	return h
}
