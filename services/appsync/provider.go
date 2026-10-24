package appsync

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/endpointstrategy"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the AppSync service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "AppSync" }

// Init initializes the AppSync service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion
	endpoint := "http://localhost:8000"

	if ctx != nil {
		if cp, ok := ctx.Config.(config.Provider); ok {
			cfg := cp.GetGlobalConfig()
			accountID = cfg.GetAccountID()
			region = cfg.GetRegion()
		}

		if ep, ok := ctx.Config.(endpointProvider); ok {
			endpoint = ep.GetEndpoint()
		}
	}

	var strategies endpointstrategy.Config
	if ctx != nil {
		strategies = endpointstrategy.From(ctx.Config)
		if err := strategies.Validate(); err != nil {
			return nil, err
		}
	}

	backend := NewInMemoryBackend(accountID, region, endpoint)
	backend.SetGraphQLEndpointStrategy(strategies.GraphQL)

	handler := NewHandler(backend)
	handler.graphqlStrategy = strategies.GraphQL
	handler.DefaultRegion = region
	handler.AccountID = accountID
	handler.EnableRegions()

	return handler, nil
}

// endpointProvider is an optional interface for config providers that expose an endpoint.
type endpointProvider interface {
	GetEndpoint() string
}

// compile-time assertion that Provider implements service.Provider.
var _ service.Provider = (*Provider)(nil)
