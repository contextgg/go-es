package es

import (
	"context"
)

// CommandBus for creating commands
type CommandBus interface {
	CommandRegistry
	CommandHandler
}

// NewCommandBus create a new bus from a registry
func NewCommandBus() CommandBus {
	return &commandBus{
		NewCommandRegistry(),
	}
}

type commandBus struct {
	CommandRegistry
}

func (b *commandBus) HandleCommand(ctx context.Context, cmd Command) error {
	// find the handler!
	handler, err := b.GetHandler(cmd)
	if err != nil {
		return err
	}
	return handler.HandleCommand(ctx, cmd)
}
