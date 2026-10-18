package cur

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for AWS Cost and Usage Reports.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "CUR" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, _ := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, homeRegion)), nil
}
