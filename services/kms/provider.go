package kms

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract KMS configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetKMSSettings() Settings
}

// Provider implements service.Provider for the KMS service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "KMS"
}

// Init initializes the KMS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var backend *InMemoryBackend
	var defaultRegion string

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithConfig(cfg.GetAccountID(), cfg.GetRegion())
		defaultRegion = cfg.GetRegion()
	} else {
		backend = NewInMemoryBackend()
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetKMSSettings()
	}

	handler := NewHandler(backend)
	handler.DefaultRegion = defaultRegion
	handler.WithJanitor(settings.JanitorInterval, ctx.JanitorTimeout)

	return handler, nil
}
