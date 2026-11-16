package azurearm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func TestAppConfigPlatformAccessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       map[string]any
		wantSKU    string
		wantAccess string
	}{
		{
			name: "disabled network", wantSKU: "standard", wantAccess: "Disabled",
			body: map[string]any{
				"sku":        map[string]any{"name": "Standard"},
				"properties": map[string]any{"publicNetworkAccess": "Disabled"},
			},
		},
		{name: "defaults", body: map[string]any{}, wantSKU: "standard"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := azurearm.NewAppConfigProvider(
				azurearm.AppConfigEndpointConfig{Override: "azconfig.localhost:1"},
				newFakeAppConfig(),
			)
			ctx := t.Context()

			_, err := p.Put(ctx, acID([]string{"configurationStores"}, "accstore"), tt.body)
			require.NoError(t, err)

			_, err = p.Put(ctx, acID([]string{"configurationStores", "replicas"}, "accstore", "east"), map[string]any{})
			require.NoError(t, err)

			got, ok := p.StoreSettings("AccStore")
			require.True(t, ok)
			assert.Equal(t, tt.wantAccess, got.PublicNetworkAccess)
			assert.Equal(t, tt.wantSKU, got.SKU)

			parent, ok := p.ReplicaParent("accstore-east")
			require.True(t, ok)
			assert.Equal(t, "accstore", parent)

			_, ok = p.ReplicaParent("accstore-west")
			assert.False(t, ok)

			_, ok = p.StoreSettings("missing")
			assert.False(t, ok)
		})
	}
}
