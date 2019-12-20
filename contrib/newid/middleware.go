package newid

import (
	"context"

	"github.com/google/uuid"

	"github.com/contextgg/go-es/es"
)

// NewMiddleware will return an es middleware so we can chain them with others
func NewMiddleware() es.CommandHandlerMiddleware {
	return func(handler es.CommandHandler) es.CommandHandler {
		return es.CommandHandlerFunc(func(ctx context.Context, cmd es.Command) error {
			id := cmd.GetAggregateID()
			if len(id) < 1 {
				if b, ok := cmd.(es.SettableID); ok {
					uid, err := uuid.NewUUID()
					if err != nil {
						return err
					}

					b.SetID(uid.String())
				}
			}

			return handler.HandleCommand(ctx, cmd)
		})
	}
}
