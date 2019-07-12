package es

import "context"

// CommandHandler for handling commands
type CommandHandler interface {
	HandleCommand(context.Context, Command) error
}
