package networkfirewall

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for AWS Network Firewall.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "NetworkFirewall" }

// Init initializes the Network Firewall backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	handler := NewHandler(NewInMemoryBackend(accountID, region))
	handler.AccountID = accountID
	handler.DefaultRegion = region
	handler.EnableRegions()

	return handler, nil
}
