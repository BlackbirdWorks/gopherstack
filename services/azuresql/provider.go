package azuresql

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// ConfigProvider extracts the Azure SQL settings from the AppContext config.
type ConfigProvider interface {
	GetAzureSQLSettings() Settings
}

// Provider implements service.Provider for Azure SQL Database.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureSQL" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureSQLSettings()
	}

	backend := NewInMemoryBackend()
	h := NewHandler(backend)

	if settings.Engine != EngineNone {
		h.launcher = newDockerLauncher(ctx.Logger, ctx.PortAlloc, settings.Image, settings.StartupTimeout)
		backend.SetLauncher(h.launcher, settings.StartupTimeout)
	}

	return h, nil
}
