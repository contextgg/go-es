package es

import (
	"context"
)

// NewSagaHandler turns an
func NewSagaHandler(b CommandBus, saga Saga, matcher EventMatcher) EventHandler {
	return &sagaHandler{b, saga, matcher}
}

type sagaHandler struct {
	bus     CommandBus
	saga    Saga
	matcher EventMatcher
}

func (s *sagaHandler) HandleEvent(ctx context.Context, evt *Event) error {
	if !s.matcher(evt) {
		return nil
	}

	cmds, err := s.saga.Run(ctx, evt)
	if err != nil {
		return err
	}

	for _, cmd := range cmds {
		if err := s.bus.HandleCommand(ctx, cmd); err != nil {
			return err
		}
	}

	return nil
}
