package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

func TestWireAzureMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		withARM bool
		withMon bool
		wantARM bool
	}{
		{name: "both present", withARM: true, withMon: true, wantARM: true},
		{name: "monitor missing is tolerated", withARM: true},
		{name: "arm missing is tolerated", withMon: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			byName := map[string]service.Registerable{}

			if tt.withARM {
				svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
				require.NoError(t, err)

				byName["AzureARM"] = svc
			}

			if tt.withMon {
				byName["AzureMonitor"] = azuremonitor.NewHandler(
					azuremonitor.NewBackend(azuremonitor.DefaultSettings()), azuremonitor.DefaultSettings())
			}

			require.NotPanics(t, func() { wireAzureMonitor(byName) })

			if !tt.wantARM {
				return
			}

			armH, ok := byName["AzureARM"].(*azurearm.Handler)
			require.True(t, ok)

			// a wired adapter turns the unavailable error into a working resource group member
			_, _, err := armH.Registry.Put(t.Context(), azurearm.ResourceID{
				SubscriptionID: "s", ResourceGroup: "rg", Namespace: "Microsoft.OperationalInsights",
				Types: []string{"workspaces"}, Names: []string{"wiring-ws"},
			}, map[string]any{"location": "westus"})
			assert.NoError(t, err)
		})
	}
}
