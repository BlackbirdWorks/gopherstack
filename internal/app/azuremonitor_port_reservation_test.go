package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	azuremonitorbackend "github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

// TestReserveFixedServicePorts_AzureMonitor checks the Logs port is pulled out of the shared PortAlloc pool.
func TestReserveFixedServicePorts_AzureMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		port                int
		wantBlockedFromPool bool
	}{
		{
			name: "default port collides with default pool range", port: azuremonitorbackend.DefaultPort,
			wantBlockedFromPool: true,
		},
		{
			name: "custom port outside a custom pool range", port: 9997, wantBlockedFromPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantBlockedFromPool, tt.port)

			cli := CLI{AzureMonitor: azuremonitorbackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(port), tt.name)
		})
	}
}
