package es

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// CommandRegistry stores the handlers for commands
type CommandRegistry interface {
	SetHandler(CommandHandler, Command) error
	GetHandler(Command) (CommandHandler, error)
	NewCommand(string) (Command, error)
}

// NewCommandRegistry creates a new CommandRegistry
func NewCommandRegistry() CommandRegistry {
	return &commandRegistry{
		registry: make(map[string]CommandHandler),
		types:    make(map[string]reflect.Type),
	}
}

type commandRegistry struct {
	sync.RWMutex
	registry map[string]CommandHandler
	types    map[string]reflect.Type
}

func (r *commandRegistry) SetHandler(handler CommandHandler, cmd Command) error {
	r.Lock()
	defer r.Unlock()

	if cmd == nil {
		return errors.New("You need to supply a command")
	}

	rawType, name := GetTypeName(cmd)
	r.registry[name] = handler
	r.types[name] = rawType
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

func (r *commandRegistry) NewCommand(name string) (Command, error) {
	for key, value := range r.types {

		if isCommandMatch(key, name) {
			i := reflect.New(value).Interface()
			return i.(Command), nil
		}
	}
	return nil, fmt.Errorf("Cannot find %s in registry", name)
}

func isCommandMatch(key, name string) bool {
	nkey := strings.ToLower(key)

	if strings.EqualFold(name, nkey) {
		return true
	}

	if strings.HasSuffix(nkey, "command") {
		mkey := nkey[:len(nkey)-7]
		return strings.EqualFold(name, mkey)
	}

	return false
}
