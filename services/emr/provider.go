package emr

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract EMR configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetEMRSettings() Settings
}

// Provider implements service.Provider for EMR.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "EMR" }

// Init initializes the EMR backend and handler.
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
		settings = cp.GetEMRSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend).WithJanitor(settings.JanitorInterval, 0, ctx.JanitorTimeout)

	return handler, nil
}
