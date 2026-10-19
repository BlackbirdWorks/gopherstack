// Package marketplacemetering emulates AWS Marketplace Metering: MeterUsage, BatchMeterUsage, RegisterUsage
// and ResolveCustomer, with entitlements derived for the Marketplace Entitlement Service.
package marketplacemetering

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for AWS Marketplace Metering.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MarketplaceMetering" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
