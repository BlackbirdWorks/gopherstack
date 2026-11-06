package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	azuretablebackend "github.com/blackbirdworks/gopherstack/services/azuretable"
)

// TestReserveFixedServicePorts_AzureTable is a sibling to
// cli_azureblob_port_reservation_test.go's TestReserveFixedServicePorts,
// covering AzureTable's own fixed-port reservation instead of restructuring
// that file's AzureBlob-only test table: services/azuretable binds its
// dedicated listener directly via net.Listen, not through PortAlloc, but its
// default port (10002) sits inside PortRangeStart/PortRangeEnd's own default
// range (10000-10100). Without reserving it, PortAlloc could still hand that
// same port number to an unrelated caller (e.g. ElastiCache), which would
// only surface later as a confusing address-in-use failure. See AZURE.md
// section 4 and pkgs/portalloc.Allocator.Reserve's doc comment.
func TestReserveFixedServicePorts_AzureTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		azurePort           int
		wantBlockedFromPool bool
	}{
		{
			name:                "default azure table port collides with default pool range",
			azurePort:           azuretablebackend.DefaultPort,
			wantBlockedFromPool: true,
		},
		{
			name:                "custom azure table port outside a custom pool range",
			azurePort:           9998,
			wantBlockedFromPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, port := fixedPortPool(t, tt.wantBlockedFromPool, tt.azurePort)

			cli := CLI{AzureTable: azuretablebackend.Settings{Port: port}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(port), tt.name)
		})
	}
}
