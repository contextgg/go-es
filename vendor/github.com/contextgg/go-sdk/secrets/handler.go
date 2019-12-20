package secrets

import (
	"context"
	"errors"
	"net/http"
)

var (
	// ErrNoAuth when no auth has been found
	ErrNoAuth = errors.New("No auth found")
)

type contextKeyType string

const authKey contextKeyType = "basic-auth"

// AuthHandlerOptional will load up a user by a token
func AuthHandlerOptional() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			username, password, ok := r.BasicAuth()
			if ok {
				// add the intro into the context.
				ctx = context.WithValue(ctx, authKey, &BasicAuthCredentials{
					Username: username,
					Password: password,
				})
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AuthFromContext will load up the Introspect from the ctx
func AuthFromContext(ctx context.Context) (*BasicAuthCredentials, error) {
	val, ok := ctx.Value(authKey).(*BasicAuthCredentials)
	if !ok {
		return nil, ErrNoAuth
	}

	return val, nil
}
