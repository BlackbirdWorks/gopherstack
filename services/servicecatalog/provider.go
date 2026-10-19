package servicecatalog

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for AWS Service Catalog.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "ServiceCatalog" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	h := NewHandler(NewInMemoryBackend(accountID, region))
	h.DefaultRegion = region

	return h, nil
}
