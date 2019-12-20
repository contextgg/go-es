package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewSagaHandler turns an
func NewSagaHandler(b es.CommandBus, saga es.Saga, matcher es.EventMatcher) es.EventHandler {
	return &sagaHandler{b, saga, matcher}
}

type sagaHandler struct {
	bus     es.CommandBus
	saga    es.Saga
	matcher es.EventMatcher
}

func (s *sagaHandler) HandleEvent(ctx context.Context, evt *es.Event) error {
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
