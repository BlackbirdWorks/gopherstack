package polly

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Amazon Polly.
type Provider struct{}

// Name returns provider name.
func (p *Provider) Name() string { return "Polly" }

// Init initializes Polly backend and REST handler.
//
//nolint:ireturn,nolintlint // service.Provider contract returns interface.
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion
	if ctx != nil {
		if cp, ok := ctx.Config.(config.Provider); ok {
			cfg := cp.GetGlobalConfig()
			accountID = cfg.GetAccountID()
			region = cfg.GetRegion()
		}
	}

	return NewHandler(NewInMemoryBackendWithConfig(accountID, region)), nil
}
