package comprehend

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Amazon Comprehend.
type Provider struct{}

// Name returns provider name.
func (p *Provider) Name() string { return "Comprehend" }

// Init creates a Comprehend backend and handler.
//
//nolint:ireturn,nolintlint // service.Provider contract returns interface.
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion
	if ctx != nil {
		if provider, ok := ctx.Config.(config.Provider); ok {
			global := provider.GetGlobalConfig()
			accountID = global.GetAccountID()
			region = global.GetRegion()
		}
	}

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
