package httpbuilder

import (
	"context"
	"os"
	"strings"

	"github.com/contextgg/go-sdk/secrets"
)

// FaaSHTTPBuilder wraps a standard HTTPBuilder
type FaaSHTTPBuilder interface {
	// SetFunction the Faas Function we want to invoke
	SetFunction(string) FaaSHTTPBuilder

	// SetMethod the method used to invoke
	SetMethod(string) FaaSHTTPBuilder

	// SetAuthBasic basic auth for the request
	SetAuthBasic(string, string) FaaSHTTPBuilder

	// SetAuthPrefix load up basic auth from ENV
	SetAuthPrefix(string) FaaSHTTPBuilder

	// SetBearerToken will set the Authorization header with a bearer token
	SetBearerToken(string) FaaSHTTPBuilder

	// SetAsync will enable an async function call
	SetAsync() FaaSHTTPBuilder

	// SetLogger so we can print stuff
	SetLogger(func(string, ...interface{})) FaaSHTTPBuilder

	// SetBody is the content for the invoke
	SetBody(interface{}) FaaSHTTPBuilder

	// AddHeader to the request
	AddHeader(string, string) FaaSHTTPBuilder

	// AddQuery to the request
	AddQuery(string, string) FaaSHTTPBuilder

	// SetOut is the output of the invoke
	SetOut(interface{}) FaaSHTTPBuilder

	// SetOut is the output of the invoke
	SetErrorString(*string) FaaSHTTPBuilder

	// Do the HTTP Request
	Do(context.Context) (int, error)
}

type faasHTTPBuilder struct {
	builder HTTPBuilder

	functionName string
	isAsync      bool
	authPrefix   string
}

func (b *faasHTTPBuilder) SetFunction(name string) FaaSHTTPBuilder {
	b.functionName = name
	return b
}

func (b *faasHTTPBuilder) SetMethod(method string) FaaSHTTPBuilder {
	b.builder.SetMethod(method)
	return b
}

func (b *faasHTTPBuilder) SetAuthBasic(username, password string) FaaSHTTPBuilder {
	b.builder.SetAuthBasic(username, password)
	return b
}

func (b *faasHTTPBuilder) SetBearerToken(token string) FaaSHTTPBuilder {
	b.builder.SetBearerToken(token)
	return b
}

func (b *faasHTTPBuilder) SetAuthPrefix(prefix string) FaaSHTTPBuilder {
	b.authPrefix = prefix
	return b
}

func (b *faasHTTPBuilder) SetAsync() FaaSHTTPBuilder {
	b.isAsync = true
	return b
}

func (b *faasHTTPBuilder) SetLogger(logger func(string, ...interface{})) FaaSHTTPBuilder {
	b.builder.SetLogger(logger)
	return b
}

func (b *faasHTTPBuilder) SetBody(body interface{}) FaaSHTTPBuilder {
	b.builder.SetBody(body)
	return b
}

func (b *faasHTTPBuilder) AddHeader(key, value string) FaaSHTTPBuilder {
	b.builder.AddHeader(key, value)
	return b
}

func (b *faasHTTPBuilder) AddQuery(key, value string) FaaSHTTPBuilder {
	b.builder.AddQuery(key, value)
	return b
}

func (b *faasHTTPBuilder) SetOut(result interface{}) FaaSHTTPBuilder {
	b.builder.SetOut(result)
	return b
}

func (b *faasHTTPBuilder) SetErrorString(body *string) FaaSHTTPBuilder {
	b.builder.SetErrorString(body)
	return b
}

func (b *faasHTTPBuilder) Do(ctx context.Context) (int, error) {
	// build the url!.
	gateway := os.Getenv("gateway_url")
	if gateway == "" {
		gateway = "http://gateway.openfaas:8080"
	}
	vertical := "function"
	if b.isAsync {
		vertical = "async-function"
	}
	url := strings.TrimSuffix(gateway, "/") + "/" + vertical + "/" + b.functionName

	b.builder.SetURL(url)

	// what about basic auth?
	creds := secrets.LoadBasicAuth(b.authPrefix)
	if creds != nil {
		b.builder.SetAuthBasic(creds.Username, creds.Password)
	}

	return b.builder.Do(ctx)
}

// NewFaaS will create a new FaaS HTTP Builder
func NewFaaS() FaaSHTTPBuilder {
	builder := New()

	return &faasHTTPBuilder{
		builder: builder,
	}
}
