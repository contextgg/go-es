package es

import (
	"errors"
	"fmt"
	"sync"
)

// CommandRegistry stores the handlers for commands
type CommandRegistry interface {
	SetHandler(CommandHandler, Command) error
	GetHandler(Command) (CommandHandler, error)
}

// NewCommandRegistry creates a new CommandRegistry
func NewCommandRegistry() CommandRegistry {
	return &commandRegistry{
		registry: make(map[string]CommandHandler),
	}
}

type commandRegistry struct {
	sync.RWMutex
	registry map[string]CommandHandler
}

func (r *commandRegistry) SetHandler(handler CommandHandler, cmd Command) error {
	r.Lock()
	defer r.Unlock()

	if cmd == nil {
		return errors.New("You need to supply a command")
	}

	_, name := GetTypeName(cmd)
	r.registry[name] = handler
	return nil
}

func (r *commandRegistry) GetHandler(cmd Command) (CommandHandler, error) {
	if cmd == nil {
		return nil, errors.New("You need to supply a command")
	}

	_, name := GetTypeName(cmd)
	handler, ok := r.registry[name]
	if !ok {
		return nil, fmt.Errorf("Cannot find %s in registry", name)
	}
	return handler, nil
}
