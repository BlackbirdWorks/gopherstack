package cognitoidp

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init receives a nil AppContext.
var ErrNilAppContext = errors.New("cognitoidp: nil AppContext")

// Provider implements service.Provider for Amazon Cognito User Pools (IDP).
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CognitoIDP" }

// Init initializes the Cognito IDP service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)
	endpoint := "http://localhost:8000"

	if ep, ok := ctx.Config.(endpointProvider); ok {
		endpoint = ep.GetEndpoint()
	}

	backend := NewInMemoryBackend(accountID, region, endpoint)
	handler := NewHandler(backend, region)
	handler.WithJanitor(0, ctx.JanitorTimeout)

	return handler, nil
}

// endpointProvider is an optional interface for config providers that expose an endpoint.
type endpointProvider interface {
	GetEndpoint() string
}

// compile-time assertion that Provider implements service.Provider.
var _ service.Provider = (*Provider)(nil)
