package es

import (
	"errors"
	"fmt"
	"sync"
)

// CommandRegister stores the handlers for commands
type CommandRegister interface {
	Add(Command, CommandHandler) error
	Get(Command) (CommandHandler, error)
}

// NewCommandRegister creates a new CommandRegister
func NewCommandRegister() CommandRegister {
	return &commandRegister{
		registry: make(map[string]CommandHandler),
	}
}

type commandRegister struct {
	sync.RWMutex
	registry map[string]CommandHandler
}

func (r *commandRegister) Add(cmd Command, handler CommandHandler) error {
	r.Lock()
	defer r.Unlock()

	if cmd == nil {
		return errors.New("You need to supply a command")
	}

	_, name := GetTypeName(cmd)
	r.registry[name] = handler
	return nil
}

func (r *commandRegister) Get(cmd Command) (CommandHandler, error) {
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
