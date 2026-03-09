package sesv2

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the SES v2 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "SESv2"
}

// Init initializes the SES v2 service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(appCtx *service.AppContext) (service.Registerable, error) {
	var backend *InMemoryBackend

	if cp, ok := appCtx.Config.(config.Provider); ok {
		backend = NewInMemoryBackendWithConfig(cp.GetGlobalConfig())
	} else {
		backend = NewInMemoryBackend()
	}

	handler := NewHandler(backend)

	return handler, nil
}
