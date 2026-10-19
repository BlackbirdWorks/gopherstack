package bcmdataexports

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for AWS Billing and Cost Management Data Exports.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "BCMDataExports" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, _ := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, homeRegion)), nil
}
