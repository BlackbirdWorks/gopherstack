package sts

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the STS service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "STS"
}

// Init initialises the STS backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithConfig(cfg.AccountID)
	} else {
		backend = NewInMemoryBackend()
	}

	handler := NewHandler(backend)

	return handler, nil
}
