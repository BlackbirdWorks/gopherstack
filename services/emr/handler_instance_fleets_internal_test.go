package emr // needs access to unexported applyInstanceFleetMod; named *_internal_test.go per house convention.

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test_ApplyInstanceFleetMod verifies ModifyInstanceFleet's mutation helper
// actually updates target/provisioned capacities -- the backend previously
// looked up the fleet and returned nil without changing anything.
func Test_ApplyInstanceFleetMod(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		mod          InstanceFleetModification
		fleet        InstanceFleet
		wantOnDemand int
		wantSpot     int
		wantProvOD   int
		wantProvSpot int
	}{
		{
			name:         "sets on-demand capacity only",
			fleet:        InstanceFleet{TargetOnDemandCapacity: 1, TargetSpotCapacity: 2},
			mod:          InstanceFleetModification{TargetOnDemandCapacity: 5},
			wantOnDemand: 5,
			wantSpot:     2,
			wantProvOD:   5,
			wantProvSpot: 0,
		},
		{
			name:         "sets spot capacity only",
			fleet:        InstanceFleet{TargetOnDemandCapacity: 1, TargetSpotCapacity: 2},
			mod:          InstanceFleetModification{TargetSpotCapacity: 7},
			wantOnDemand: 1,
			wantSpot:     7,
			wantProvOD:   0,
			wantProvSpot: 7,
		},
		{
			name:         "sets both capacities",
			fleet:        InstanceFleet{},
			mod:          InstanceFleetModification{TargetOnDemandCapacity: 3, TargetSpotCapacity: 4},
			wantOnDemand: 3,
			wantSpot:     4,
			wantProvOD:   3,
			wantProvSpot: 4,
		},
		{
			name:         "zero mod leaves existing capacities untouched",
			fleet:        InstanceFleet{TargetOnDemandCapacity: 9, TargetSpotCapacity: 9},
			mod:          InstanceFleetModification{},
			wantOnDemand: 9,
			wantSpot:     9,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := tt.fleet
			applyInstanceFleetMod(&f, tt.mod)

			assert.Equal(t, tt.wantOnDemand, f.TargetOnDemandCapacity)
			assert.Equal(t, tt.wantSpot, f.TargetSpotCapacity)

			if tt.mod.TargetOnDemandCapacity > 0 {
				assert.Equal(t, tt.wantProvOD, f.ProvisionedOnDemandCapacity)
			}

			if tt.mod.TargetSpotCapacity > 0 {
				assert.Equal(t, tt.wantProvSpot, f.ProvisionedSpotCapacity)
			}
		})
	}
}
