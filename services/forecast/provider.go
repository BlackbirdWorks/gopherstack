package forecast

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init receives nil context.
var ErrNilAppContext = errors.New("forecast: nil AppContext")

// Provider initializes Amazon Forecast.
type Provider struct{}

// Name returns provider name.
func (p *Provider) Name() string { return "Forecast" }

// Init initializes Amazon Forecast backend and handler.
//
//nolint:ireturn,nolintlint // Provider contract returns service interface.
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID := config.DefaultAccountID
	region := config.DefaultRegion
	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.GetAccountID()
		region = cfg.GetRegion()
	}

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
