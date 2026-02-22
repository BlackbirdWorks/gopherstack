package secretsmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Secrets Manager service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "SecretsManager"
}

// Init initializes the Secrets Manager service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend, ctx.Logger)

	return handler, nil
}
