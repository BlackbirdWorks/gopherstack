package cognitoidp

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Amazon Cognito User Pools (IDP).
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CognitoIDP" }

// Init initializes the Cognito IDP service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := "000000000000"
	region := "us-east-1"
	endpoint := "http://localhost:8000"

	if ctx != nil {
		if cp, ok := ctx.Config.(config.Provider); ok {
			cfg := cp.GetGlobalConfig()
			accountID = cfg.AccountID
			region = cfg.Region
		}

		if ep, ok := ctx.Config.(endpointProvider); ok {
			endpoint = ep.GetEndpoint()
		}
	}

	backend := NewInMemoryBackend(accountID, region, endpoint)
	handler := NewHandler(backend, region)

	return handler, nil
}

// endpointProvider is an optional interface for config providers that expose an endpoint.
type endpointProvider interface {
	GetEndpoint() string
}

// compile-time assertion that Provider implements service.Provider.
var _ service.Provider = (*Provider)(nil)
