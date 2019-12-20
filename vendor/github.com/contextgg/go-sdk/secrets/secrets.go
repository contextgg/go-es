package secrets

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// MustReadSecret sets up a secret with a fallback value
func MustReadSecret(key, fallback string) string {
	secret, err := ReadSecret(key)
	if err != nil {
		secret = os.Getenv(key)
	}
	if secret == "" {
		return fallback
	}
	return secret
}

// ReadSecret reads a secret from /var/openfaas/secrets or from
// env-var 'secret_mount_path' if set.
func ReadSecret(key string) (string, error) {
	basePath := "/var/openfaas/secrets/"
	if len(os.Getenv("secret_mount_path")) > 0 {
		basePath = os.Getenv("secret_mount_path")
	}

	readPath := path.Join(basePath, key)
	secretBytes, readErr := ioutil.ReadFile(readPath)
	if readErr != nil {
		return "", fmt.Errorf("unable to read secret: %s, error: %s", readPath, readErr)
	}
	val := strings.TrimSpace(string(secretBytes))
	return val, nil
}
