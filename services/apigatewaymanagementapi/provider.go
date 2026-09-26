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
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	if ctx != nil && ctx.JanitorCtx != nil {
		janitor := NewJanitor(backend, 0, 0)
		janitor.TaskTimeout = ctx.JanitorTimeout

		go janitor.Run(ctx.JanitorCtx)
	}

	return handler, nil
}
