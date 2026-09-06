package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	cosmosdbbackend "github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// TestReserveFixedServicePorts_CosmosDB is a sibling to
// cli_azuretable_port_reservation_test.go's TestReserveFixedServicePorts_AzureTable,
// covering CosmosDB's own fixed-port reservation. Unlike AzureBlob (10000),
// AzureQueue (10001), and AzureTable (10002) -- all of which sit INSIDE
// PortRangeStart/PortRangeEnd's own default range (10000-10100), so their
// port-reservation tests only need one "blocked" case -- CosmosDB's default
// port (8081, the real Cosmos DB Local Emulator's own published default)
// sits OUTSIDE that default range entirely, exactly like services/iot's
// MQTT broker default (1883; see AZURE.md section 4). So this table is
// deliberately the INVERSE of AzureTable's: the default-port case is
// wantBlockedFromPool: false (8081 is simply outside 10000-10100, a no-op
// Reserve call), and a SEPARATE custom-pool-range case
// (rangeStart:8000, rangeEnd:8100) demonstrates that 8081 DOES get
// reserved once an operator's custom range happens to include it --
// proving the reservation is correct and cheap even though it's a no-op
// against the shipped defaults. See cli.go's reserveFixedServicePorts and
// services/cosmosdb/settings.go's DefaultPort doc comment for the full
// rationale.
func TestReserveFixedServicePorts_CosmosDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		cosmosPort          int
		rangeStart          int
		rangeEnd            int
		wantBlockedFromPool bool
	}{
		{
			name:       "default cosmosdb port falls outside the default pool range",
			cosmosPort: cosmosdbbackend.DefaultPort, rangeStart: 10000, rangeEnd: 10100,
			wantBlockedFromPool: false,
		},
		{
			name:       "default cosmosdb port falls inside a custom pool range",
			cosmosPort: cosmosdbbackend.DefaultPort, rangeStart: 8000, rangeEnd: 8100,
			wantBlockedFromPool: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			alloc, err := portalloc.New(tt.rangeStart, tt.rangeEnd)
			require.NoError(t, err)

			cli := CLI{CosmosDB: cosmosdbbackend.Settings{Port: tt.cosmosPort}}
			reserveFixedServicePorts(t.Context(), slog.Default(), alloc, cli)

			assert.Equal(t, tt.wantBlockedFromPool, alloc.IsAllocated(tt.cosmosPort), tt.name)
		})
	}
}
