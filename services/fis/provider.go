package fis

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract FIS configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetFISSettings() Settings
}

// Provider implements service.Provider for the FIS service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "FIS" }

// Init initializes the FIS service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetFISSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.DefaultRegion = region
	handler.AccountID = accountID
	handler.WithJanitor(settings.JanitorInterval, settings.ExperimentTTL, ctx.JanitorTimeout)

	return handler, nil
}
