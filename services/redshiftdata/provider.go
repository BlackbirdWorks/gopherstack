package redshiftdata

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when a nil AppContext is passed.
var ErrNilAppContext = errors.New("nil AppContext passed to RedshiftData Provider.Init")

// ConfigProvider is a private interface to extract Redshift Data configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetRedshiftDataSettings() Settings
}

// Provider implements service.Provider for AWS Redshift Data.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "RedshiftData" }

// Init initializes the Redshift Data service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetRedshiftDataSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.WithJanitor(settings.JanitorInterval, settings.StatementTTL, ctx.JanitorTimeout)

	return handler, nil
}
