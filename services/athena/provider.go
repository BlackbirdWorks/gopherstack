package athena

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract Athena configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetAthenaSettings() Settings
}

// Provider implements service.Provider for the Athena service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Athena" }

// Init initializes the Athena backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var settings Settings

	accountID, region := service.AccountRegionOrDefault(ctx)

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAthenaSettings()
	}

	backend := NewInMemoryBackend(region, accountID)
	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, settings.ExecutionTTL, ctx.JanitorTimeout)

	return handler, nil
}
