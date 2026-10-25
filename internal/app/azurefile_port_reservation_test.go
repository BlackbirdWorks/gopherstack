package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	azurefilebackend "github.com/blackbirdworks/gopherstack/services/azurefile"
)

// TestReserveFixedServicePorts_AzureFile checks the Files port is pulled out of the shared PortAlloc pool.
func TestReserveFixedServicePorts_AzureFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		azurePort           int
		rangeStart          int
		rangeEnd            int
		wantBlockedFromPool bool
	}{
		{
			name:      "default azure files port collides with default pool range",
			azurePort: azurefilebackend.DefaultPort, rangeStart: 10000, rangeEnd: 10100,
			wantBlockedFromPool: true,
		},
		{
			name:      "custom azure files port outside a custom pool range",
			azurePort: 9998, rangeStart: 10000, rangeEnd: 10100,
			wantBlockedFromPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{AzureFile: azurefilebackend.Settings{Port: tt.azurePort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(tt.azurePort), tt.name)
		})
	}
}
