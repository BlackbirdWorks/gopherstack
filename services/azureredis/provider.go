package azureredis

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// ConfigProvider extracts the Azure Cache for Redis settings from the AppContext config.
type ConfigProvider interface {
	GetAzureRedisSettings() Settings
}

// Provider implements service.Provider for Azure Cache for Redis.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureRedis" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureRedisSettings()
	}

	backend := NewInMemoryBackend(settings)
	if ctx.PortAlloc != nil {
		backend.SetPortPool(ctx.PortAlloc)
	}

	return NewHandler(backend), nil
}
