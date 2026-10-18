package rum

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for Amazon CloudWatch RUM.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "RUM" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
