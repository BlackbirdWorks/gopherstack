package sts

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract STS configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetSTSSettings() Settings
}

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
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithConfig(cfg.GetAccountID())
		backend.SetStrictConditions(cfg.IsIAMEnforced())
	} else {
		backend = NewInMemoryBackend()
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetSTSSettings()
	}

	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, ctx.JanitorTimeout)

	return handler, nil
}
