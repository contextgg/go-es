package hydra

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

type contextKeyType string

const authKey contextKeyType = "auth"

var (
	// ErrNoAuth when no auth has been found
	ErrNoAuth = errors.New("No auth found")

	// ErrNotActive when a token isn't active
	ErrNotActive = errors.New("Not active")

	// ErrNotAccessToken when the token isn't an access token
	ErrNotAccessToken = errors.New("Token isn't an access_token")
)

func load(ctx context.Context, hydraURL, authorization string) (*Introspect, error) {
	if !strings.HasPrefix(authorization, "Bearer ") {
		return nil, ErrNoAuth
	}

	token := strings.TrimPrefix(authorization, "Bearer ")

	result, err := IntrospectToken(ctx, hydraURL, token)
	if err != nil {
		return nil, err
	}
	if !result.Active {
		return result, ErrNotActive
	}
	if !result.IsAccessToken() {
		return result, ErrNotAccessToken
	}
	return result, nil
}

// AuthFromContext will load up the Introspect from the ctx
func AuthFromContext(ctx context.Context) (*Introspect, error) {
	val, ok := ctx.Value(authKey).(*Introspect)
	if !ok {
		return nil, ErrNoAuth
	}

	return val, nil
}

// AuthHandler will load up a user by a token
func AuthHandler(hydraURL string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			authorization := r.Header.Get("Authorization")

			intro, err := load(ctx, hydraURL, authorization)
			if err == ErrNoAuth {
				http.Error(w, "", http.StatusUnauthorized)
				return
			}
			if err != nil {
				http.Error(w, "", http.StatusForbidden)
				return
			}
			// add the intro into the context.
			ctx = context.WithValue(ctx, authKey, intro)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AuthHandlerOptional will load up a user by a token
func AuthHandlerOptional(hydraURL string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			authorization := r.Header.Get("Authorization")

			intro, err := load(ctx, hydraURL, authorization)
			if err == nil && intro != nil {
				// add the intro into the context.
				ctx = context.WithValue(ctx, authKey, intro)
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
