package emr

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("EMR provider: nil AppContext")

// ConfigProvider is a private interface to extract EMR configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetEMRSettings() Settings
}

// Provider implements service.Provider for EMR.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "EMR" }

// Init initializes the EMR backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetEMRSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend).WithJanitor(settings.JanitorInterval, settings.TerminatedTTL, ctx.JanitorTimeout)

	return handler, nil
}
