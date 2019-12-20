package hydra

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/contextgg/go-sdk/httpbuilder"
)

// IntrospectToken will introspect the token with hydra
func IntrospectToken(ctx context.Context, hydraURL, token string) (*Introspect, error) {
	var result Introspect
	data := url.Values{}
	data.Set("token", token)

	status, err := httpbuilder.New().
		SetURL(hydraURL).
		SetMethod(http.MethodPost).
		AppendPath("/oauth2/introspect").
		AddHeader("Accept", "application/json").
		SetBody(data).
		SetOut(&result).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.New("Invalid status code")
	}

	return &result, nil
}
