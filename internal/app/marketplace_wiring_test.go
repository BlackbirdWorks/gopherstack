package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	marketplaceentitlementbackend "github.com/blackbirdworks/gopherstack/services/marketplaceentitlementservice"
	marketplacemeteringbackend "github.com/blackbirdworks/gopherstack/services/marketplacemetering"
)

func TestWireMarketplaceEntitlements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wired     bool
		wantCount int
	}{
		{name: "wired", wired: true, wantCount: 1},
		{name: "entitlement handler absent", wired: false, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meter := marketplacemeteringbackend.NewHandler(
				marketplacemeteringbackend.NewInMemoryBackend("000000000000", "us-east-1"),
			)
			ent := marketplaceentitlementbackend.NewHandler(
				marketplaceentitlementbackend.NewInMemoryBackend("000000000000", "us-east-1"),
			)

			byName := map[string]service.Registerable{"MarketplaceMetering": meter}
			if tt.wired {
				byName["MarketplaceEntitlementService"] = ent
			}

			wireMarketplaceEntitlements(byName)

			one := int32(1)
			require.NoError(t, meter.Backend.RegisterProduct(marketplacemeteringbackend.ProductSpec{
				Code: "wired-product", Dimensions: []marketplacemeteringbackend.Dimension{
					{Name: "Seats", Value: &marketplaceentitlementbackend.Value{IntegerValue: &one}},
				},
			}))

			_, err := meter.Backend.Subscribe("wired-product", "111122223333")
			require.NoError(t, err)

			out, err := ent.Backend.GetEntitlements(
				&marketplaceentitlementbackend.GetEntitlementsInput{ProductCode: "wired-product"},
			)
			require.NoError(t, err)
			assert.Len(t, out.Entitlements, tt.wantCount)
		})
	}
}
