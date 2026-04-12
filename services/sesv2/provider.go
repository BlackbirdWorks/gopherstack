package sesv2

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("sesv2: nil app context")

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
	if appCtx == nil {
		return nil, ErrNilAppContext
	}

	var backend *InMemoryBackend

	if cp, ok := appCtx.Config.(config.Provider); ok {
		backend = NewInMemoryBackendWithConfig(cp.GetGlobalConfig())
	} else {
		backend = NewInMemoryBackend()
	}

	handler := NewHandler(backend)

	return handler, nil
}
