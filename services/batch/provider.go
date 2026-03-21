package batch

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract Batch configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetBatchSettings() Settings
}

// Provider implements service.Provider for Batch.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Batch" }

// Init initializes the Batch backend and handler.
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

	settings := Settings{
		JanitorInterval:   defaultBatchJanitorInterval,
		InactiveJobDefTTL: defaultBatchInactiveJobDefTTL,
	}
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetBatchSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, settings.InactiveJobDefTTL, ctx.JanitorTimeout)

	return handler, nil
}
