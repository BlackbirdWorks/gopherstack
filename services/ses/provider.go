package ses

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when the AppContext is nil.
var ErrNilAppContext = errors.New("ses: AppContext is nil")

// ConfigProvider is a private interface to extract SES configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetSESSettings() Settings
}

// Provider implements service.Provider for the SES service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "SES"
}

// Init initializes the SES service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}
	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetSESSettings()
	}

	backend := NewInMemoryBackend().WithEmailTTL(settings.EmailTTL)
	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, ctx.JanitorTimeout)

	return handler, nil
}
