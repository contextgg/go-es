package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewCommandBus create a new bus from a registry
func NewCommandBus(registry es.CommandRegister) es.CommandBus {
	return &commandBus{
		register: registry,
	}
}

type commandBus struct {
	register es.CommandRegister
}

func (b *commandBus) HandleCommand(ctx context.Context, cmd es.Command) error {
	// find the handler!
	handler, err := b.register.Get(cmd)
	if err != nil {
		return err
	}
	return handler.HandleCommand(ctx, cmd)
}

// Close underlying connection
func (b *commandBus) Close() {
}
