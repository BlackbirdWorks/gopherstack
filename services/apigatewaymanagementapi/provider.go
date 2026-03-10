package apigatewaymanagementapi

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the API Gateway Management API service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "APIGatewayManagementAPI" }

// Init initialises the API Gateway Management API backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
