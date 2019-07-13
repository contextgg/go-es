package es

// CommandBus for creating commands
type CommandBus interface {
	CommandHandler
	Close()
}
