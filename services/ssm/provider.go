package ssm

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ssm: nil app context")

// ConfigProvider is a private interface to extract SSM configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetSSMSettings() Settings
}

// Provider implements service.Provider for the SSM Parameter Store service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "SSM"
}

// Init initializes the SSM service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetSSMSettings()
	}

	backend := NewInMemoryBackend().WithCommandTTL(settings.CommandTTL)
	handler := NewHandler(backend).WithJanitor(settings.JanitorInterval, ctx.JanitorTimeout)

	return handler, nil
}
