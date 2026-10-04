package appconfigdata

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the AppConfigData service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AppConfigData" }

// Init initialises the AppConfigData backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	region := config.DefaultRegion
	if ctx != nil {
		_, region = service.AccountRegionOrDefault(ctx)
	}

	backend := NewInMemoryBackend()
	handler := NewHandler(backend)
	handler.EnableRegions(region)

	return handler, nil
}
