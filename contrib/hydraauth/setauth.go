package hydraauth

import (
	"github.com/contextgg/go-sdk/hydra"
)

// CommandAuth to help set the auth
type CommandAuth interface {
	SetAuth(*hydra.Introspect)
}

// BaseAuthCommand to populate a command with some auth stuff
type BaseAuthCommand struct {
	Introspect *hydra.Introspect `json:"-" validate:"required"`
}

// SetAuth will populate the object with the auth
func (cmd *BaseAuthCommand) SetAuth(introspect *hydra.Introspect) {
	cmd.Introspect = introspect
}
