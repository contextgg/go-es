package validator

import (
	"context"

	"gopkg.in/go-playground/validator.v9"

	"github.com/contextgg/go-es/es"
)

// NewMiddleware will return an es middleware so we can chain them with others
func NewMiddleware(validate *validator.Validate) es.CommandHandlerMiddleware {
	return func(handler es.CommandHandler) es.CommandHandler {
		return es.CommandHandlerFunc(func(ctx context.Context, cmd es.Command) error {
			// Just terminate the request if the input is not valid
			if err := validate.Struct(cmd); err != nil {
				return err
			}

			return handler.HandleCommand(ctx, cmd)
		})
	}
}
