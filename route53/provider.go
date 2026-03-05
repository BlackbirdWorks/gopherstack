package route53

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Route 53 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Route53" }

// Init initializes the Route 53 service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
