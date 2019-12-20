package basicauth

import (
	"context"
	"errors"

	"github.com/contextgg/go-es/es"
	"github.com/contextgg/go-sdk/secrets"
)

var (
	// ErrInvalidCredentials when the supplied credentials don't match
	ErrInvalidCredentials = errors.New("Basic auth did not compute")
)

// NewMiddleware will return an es middleware so we can chain them with others
func NewMiddleware(creds *secrets.BasicAuthCredentials) es.CommandHandlerMiddleware {
	return func(handler es.CommandHandler) es.CommandHandler {
		return es.CommandHandlerFunc(func(ctx context.Context, cmd es.Command) error {
			cur, err := secrets.AuthFromContext(ctx)
			if err != nil {
				return err
			}

			if !creds.Equals(cur) {
				return ErrInvalidCredentials
			}

			return handler.HandleCommand(ctx, cmd)
		})
	}
}
