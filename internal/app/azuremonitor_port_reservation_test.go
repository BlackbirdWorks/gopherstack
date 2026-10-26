package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	azuremonitorbackend "github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

// TestReserveFixedServicePorts_AzureMonitor checks the Logs port is pulled out of the shared PortAlloc pool.
func TestReserveFixedServicePorts_AzureMonitor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		port                int
		rangeStart          int
		rangeEnd            int
		wantBlockedFromPool bool
	}{
		{
			name: "default port collides with default pool range", port: azuremonitorbackend.DefaultPort,
			rangeStart: 10000, rangeEnd: 10100, wantBlockedFromPool: true,
		},
		{
			name: "custom port outside a custom pool range", port: 9997,
			rangeStart: 10000, rangeEnd: 10100, wantBlockedFromPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{AzureMonitor: azuremonitorbackend.Settings{Port: tt.port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(tt.port), tt.name)
		})
	}
}
