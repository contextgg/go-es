package es

// CommandBus for creating commands
type CommandBus interface {
	CommandRegistry
	CommandHandler
}
