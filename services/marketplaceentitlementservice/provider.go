// Package marketplaceentitlementservice emulates the AWS Marketplace Entitlement Service (GetEntitlements).
package marketplaceentitlementservice

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for the Marketplace Entitlement Service.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "MarketplaceEntitlementService" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	return NewHandler(NewInMemoryBackend(accountID, region)), nil
}
