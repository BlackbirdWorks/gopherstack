package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	marketplaceentitlementbackend "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
	marketplacemeteringbackend "github.com/blackbirdworks/gopherstack/services/marketplacemetering"
)

// wireMarketplaceEntitlements makes Marketplace Metering grant the entitlements that
// Marketplace Entitlement Service reads, so ResolveCustomer subscriptions show up in GetEntitlements.
func wireMarketplaceEntitlements(byName map[string]service.Registerable) {
	meter, mok := byName["MarketplaceMetering"].(*marketplacemeteringbackend.Handler)
	ent, eok := byName["MarketplaceEntitlementService"].(*marketplaceentitlementbackend.Handler)

	if mok && eok {
		meter.Backend.SetEntitlementSink(ent.Backend)
	}
}
