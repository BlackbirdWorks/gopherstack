package apigateway

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the API Gateway service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "APIGateway" }

// Init initializes the API Gateway service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
