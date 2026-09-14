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

// TestModifyFleet_ConvergesCapacity covers gopherstack-a3qy: ModifyFleet must
// launch or terminate instances to converge on the new TotalTargetCapacity,
// honoring ExcessCapacityTerminationPolicy on the way down.
func TestModifyFleet_ConvergesCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		excessPolicy string
		modifyPolicy string
		createCap    int
		modifyCap    int
		wantCount    int
	}{
		{name: "scale up launches instances", createCap: 2, modifyCap: 5, wantCount: 5},
		{name: "scale down terminates by default", createCap: 5, modifyCap: 2, wantCount: 2},
		{
			name:         "scale down honors no-termination policy",
			createCap:    5,
			excessPolicy: "no-termination",
			modifyCap:    2,
			wantCount:    5,
		},
		{
			name:         "modify can raise the no-termination policy back to termination",
			createCap:    5,
			excessPolicy: "no-termination",
			modifyCap:    2,
			modifyPolicy: "termination",
			wantCount:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			f, _, err := b.CreateFleet(ec2.FleetCreateInput{
				Type:                            "maintain",
				TotalTargetCapacity:             tt.createCap,
				ExcessCapacityTerminationPolicy: tt.excessPolicy,
				LaunchTemplateConfigs:           []ec2.FleetLaunchTemplateConfig{{LaunchTemplateID: "lt-doesnotexist"}},
			})
			require.NoError(t, err)

			instances, err := b.DescribeFleetInstances(f.FleetID, nil)
			require.NoError(t, err)
			require.Len(t, instances, tt.createCap)

			err = b.ModifyFleet(f.FleetID, tt.modifyCap, tt.modifyPolicy)
			require.NoError(t, err)

			instances, err = b.DescribeFleetInstances(f.FleetID, nil)
			require.NoError(t, err)
			assert.Len(t, instances, tt.wantCount)

			fleets := b.DescribeFleets([]string{f.FleetID})
			require.Len(t, fleets, 1)
			assert.Equal(t, tt.modifyCap, fleets[0].TotalTargetCapacity)
		})
	}
}

// TestDeleteVpc_SecondaryIndexes verifies that DeleteVpc correctly removes subnet,
// route table, and security group secondary index entries so they don't linger.
