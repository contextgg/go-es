package secrets

import (
	"errors"
	"net/http"
)

var (
	// ErrCredsNotSupplied when the server doesn't supply creds
	ErrCredsNotSupplied = errors.New("Credentials are not supplied")
	// ErrBasicAuthNotSupplied when the request doesn't have creds
	ErrBasicAuthNotSupplied = errors.New("Basic auth not supplied")
	// ErrUsernameNotEqual when the username in the request doesn't equal the expected
	ErrUsernameNotEqual = errors.New("Username is not correct")
	// ErrPasswordNotEqual when the password in the request doesn't equal the expected
	ErrPasswordNotEqual = errors.New("Password is not correct")
)

// BasicAuthCredentials for credentials
type BasicAuthCredentials struct {
	Username string
	Password string
}

// Equals checks if username and password are the same
func (b *BasicAuthCredentials) Equals(a *BasicAuthCredentials) bool {
	if b == a {
		return true
	}
	if b == nil || a == nil {
		return false
	}
	return b.Username == a.Username && b.Password == a.Password
}

// LoadBasicAuth will load the secrets from disk
func LoadBasicAuth(prefix string) *BasicAuthCredentials {
	if prefix == "" {
		return nil
	}

	usernameKey := prefix + "-basic-auth-username"
	username := MustReadSecret(usernameKey, "")
	if username == "" {
		return nil
	}

	passwordKey := prefix + "-basic-auth-password"
	password := MustReadSecret(passwordKey, "")
	if password == "" {
		return nil
	}

	return &BasicAuthCredentials{
		Username: username,
		Password: password,
	}
}

// VerifyBasicAuth checks the request basic auth against our creds
func VerifyBasicAuth(r *http.Request, creds *BasicAuthCredentials) error {
	if creds == nil {
		return ErrCredsNotSupplied
	}

	u, p, ok := r.BasicAuth()
	if !ok {
		return ErrBasicAuthNotSupplied
	}

	if u != creds.Username {
		return ErrUsernameNotEqual
	}

	if p != creds.Password {
		return ErrPasswordNotEqual
	}

	return nil
}
