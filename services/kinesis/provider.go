package kinesis

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when the AppContext is nil.
var ErrNilAppContext = errors.New("kinesis: AppContext must not be nil")

// ConfigProvider is a private interface to extract Kinesis configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetKinesisSettings() Settings
}

// Provider implements service.Provider for the Kinesis service.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "Kinesis" }

// Init initializes the Kinesis service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var backend *InMemoryBackend
	var defaultRegion, accountID string

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithConfig(cfg.GetAccountID(), cfg.GetRegion())
		defaultRegion = cfg.GetRegion()
		accountID = cfg.GetAccountID()
	} else {
		backend = NewInMemoryBackend()
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetKinesisSettings()
	}

	handler := NewHandler(backend)
	handler.DefaultRegion = defaultRegion
	handler.AccountID = accountID
	handler.WithJanitor(settings.JanitorInterval, ctx.JanitorTimeout)

	return handler, nil
}
