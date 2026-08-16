package ec2_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestCreateFleet_ReturnsFleetsId verifies that CreateFleet returns a proper
// fleetId field (not just <return>true</return>).
func TestCreateFleet_ReturnsFleetsId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fleetType string
	}{
		{"maintain fleet", "maintain"},
		{"request fleet", "request"},
		{"instant fleet", "instant"},
		{"default type", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			vals := url.Values{
				"Action":  {"CreateFleet"},
				"Version": {"2016-11-15"},
				"TargetCapacitySpecification.TotalTargetCapacity": {"1"},
			}
			if tc.fleetType != "" {
				vals.Set("Type", tc.fleetType)
			}

			resp, err := dispatchHandler(h, vals)
			require.NoError(t, err)

			// Must have a fleetId, not just <return>true</return>.
			fleetID := accuracyExtractXMLValue(resp, "fleetId")
			assert.NotEmpty(t, fleetID, "CreateFleet response must include fleetId")
			assert.True(t, strings.HasPrefix(fleetID, "fleet-"),
				"fleetId must be fleet-prefixed, got %q", fleetID)

			// Must not be just a stub return.
			assert.NotContains(t, resp, "<return>true</return>",
				"CreateFleet must not return stub boolean response")
		})
	}
}

// TestDeleteVpc_SecondaryIndexes verifies that DeleteVpc correctly removes subnet,
// route table, and security group secondary index entries so they don't linger.
