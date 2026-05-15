package ec2

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("AppContext is required")

// ConfigProvider is a private interface to extract EC2 configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetEC2Settings() Settings
}

// Provider implements service.Provider for the EC2 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "EC2"
}

// Init initializes the EC2 service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var accountID, region string

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.GetAccountID()
		region = cfg.GetRegion()
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetEC2Settings()
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.Region = region

	handler.WithJanitor(
		settings.JanitorInterval,
		settings.TerminatedTTL,
		settings.CancelledSpotTTL,
		ctx.JanitorTimeout,
	)

	return handler, nil
}
