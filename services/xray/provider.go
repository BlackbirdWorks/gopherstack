package xray

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract X-Ray configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetXRaySettings() Settings
}

// Provider implements service.Provider for the X-Ray service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Xray" }

// Init initializes the X-Ray service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetXRaySettings()
	}

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, settings.TraceTTL, ctx.JanitorTimeout)

	return handler, nil
}
