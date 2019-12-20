package hydraauth

import (
	"context"

	"github.com/contextgg/go-sdk/hydra"

	"github.com/contextgg/go-es/es"
)

// NewMiddleware will return an es middleware so we can chain them with others
func NewMiddleware() es.CommandHandlerMiddleware {
	return func(handler es.CommandHandler) es.CommandHandler {
		return es.CommandHandlerFunc(func(ctx context.Context, cmd es.Command) error {
			if set, ok := cmd.(CommandAuth); ok {
				intro, err := hydra.AuthFromContext(ctx)
				if err != nil {
					return err
				}

				set.SetAuth(intro)
			}

			return handler.HandleCommand(ctx, cmd)
		})
	}
}
