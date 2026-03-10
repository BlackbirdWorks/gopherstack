package appconfigdata

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the AppConfigData service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AppConfigData" }

// Init initialises the AppConfigData backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(_ *service.AppContext) (service.Registerable, error) {
	backend := NewInMemoryBackend()
	handler := NewHandler(backend)

	return handler, nil
}
