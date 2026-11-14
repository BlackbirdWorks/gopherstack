package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	azurefilebackend "github.com/blackbirdworks/gopherstack/services/azurefile"
)

// TestReserveFixedServicePorts_AzureFile checks the Files port is pulled out of the shared PortAlloc pool.
func TestReserveFixedServicePorts_AzureFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		azurePort           int
		wantBlockedFromPool bool
	}{
		{
			name:                "default azure files port collides with default pool range",
			azurePort:           azurefilebackend.DefaultPort,
			wantBlockedFromPool: true,
		},
		{
			name:                "custom azure files port outside a custom pool range",
			azurePort:           9998,
			wantBlockedFromPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantBlockedFromPool, tt.azurePort)

			cli := CLI{AzureFile: azurefilebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(port), tt.name)
		})
	}
}
