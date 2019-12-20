package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewCommandBus create a new bus from a registry
func NewCommandBus() es.CommandBus {
	return &commandBus{
		es.NewCommandRegistry(),
	}
}

type commandBus struct {
	es.CommandRegistry
}

func (b *commandBus) HandleCommand(ctx context.Context, cmd es.Command) error {
	// find the handler!
	handler, err := b.GetHandler(cmd)
	if err != nil {
		return err
	}
	return handler.HandleCommand(ctx, cmd)
}
