package amplify

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Amplify service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Amplify" }

// Init initializes the Amplify service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := config.DefaultAccountID
	region := config.DefaultRegion

	if ctx != nil {
		if cp, ok := ctx.Config.(config.Provider); ok {
			cfg := cp.GetGlobalConfig()
			accountID = cfg.AccountID
			region = cfg.Region
		}
	}

	backend := NewInMemoryBackend(accountID, region)
	handler := NewHandler(backend)
	handler.DefaultRegion = region
	handler.AccountID = accountID

	return handler, nil
}

// compile-time assertion that Provider implements service.Provider.
var _ service.Provider = (*Provider)(nil)
