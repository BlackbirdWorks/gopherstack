package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- EC2 Fleet ----.
func TestFleet(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var fleetID string

	t.Run("create fleet", func(t *testing.T) { //nolint:paralleltest // existing issue.
		f, launched, err := b.CreateFleet(ec2.FleetCreateInput{
			Type:                  "maintain",
			TotalTargetCapacity:   5,
			LaunchTemplateConfigs: []ec2.FleetLaunchTemplateConfig{{LaunchTemplateID: "lt-doesnotexist"}},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, f.FleetID)
		assert.Equal(t, "active", f.FleetState)
		assert.Equal(t, "maintain", f.FleetType)
		assert.Equal(t, 5, f.TotalTargetCapacity)
		assert.Len(t, f.InstanceIDs, 5, "CreateFleet must launch TotalTargetCapacity instances")
		require.Len(t, launched, 1)
		assert.Len(t, launched[0].InstanceIDs, 5)
		fleetID = f.FleetID
	})

	t.Run("describe returns created fleet", func(t *testing.T) { //nolint:paralleltest // existing issue.
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		assert.Equal(t, "active", fleets[0].FleetState)
		assert.Len(t, fleets[0].InstanceIDs, 5)
	})

	t.Run("describe fleet instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		instances, err := b.DescribeFleetInstances(fleetID, nil)
		require.NoError(t, err)
		require.Len(t, instances, 5, "must return the instances CreateFleet actually launched")

		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)

		gotIDs := make([]string, 0, len(instances))
		for _, inst := range instances {
			gotIDs = append(gotIDs, inst.InstanceID)
			assert.NotEmpty(t, inst.InstanceType)
		}

		assert.ElementsMatch(t, fleets[0].InstanceIDs, gotIDs)
	})

	t.Run("describe fleet history returns a real event", func(t *testing.T) { //nolint:paralleltest // existing issue.
		records, err := b.DescribeFleetHistory(fleetID, time.Time{}, "")
		require.NoError(t, err)
		require.NotEmpty(t, records, "CreateFleet must record a history event")
		assert.Equal(t, "fleet-change", records[0].EventType)
	})

	t.Run("describe all fleets", func(t *testing.T) { //nolint:paralleltest // existing issue.
		fleets := b.DescribeFleets(nil)
		assert.NotEmpty(t, fleets)
	})

	t.Run("modify fleet capacity", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyFleet(fleetID, 10, ""))
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		assert.Equal(t, 10, fleets[0].TotalTargetCapacity)
	})

	t.Run("modify fleet excess policy", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyFleet(fleetID, 0, "no-termination"))
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		assert.Equal(t, "no-termination", fleets[0].ExcessCapacityTerminationPolicy)
	})

	t.Run("delete fleet terminates its instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		instanceIDs := fleets[0].InstanceIDs
		require.NotEmpty(t, instanceIDs)

		deleted := b.DeleteFleets([]string{fleetID}, true)
		require.Len(t, deleted, 1)
		assert.Equal(t, fleetID, deleted[0].FleetID)
		assert.Equal(t, "active", deleted[0].PreviousFleetState)

		fleetsAfter := b.DescribeFleets([]string{fleetID})
		assert.Empty(t, fleetsAfter)

		for _, id := range instanceIDs {
			insts := b.DescribeInstances([]string{id}, "")
			require.Len(t, insts, 1)
			assert.Equal(t, "terminated", insts[0].State.Name,
				"DeleteFleets(terminateInstances=true) must terminate the fleet's instances")
		}
	})

	t.Run("delete non-existent fleet returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		deleted := b.DeleteFleets([]string{"fleet-nonexistent"}, true)
		assert.Empty(t, deleted)
	})

	t.Run("modify non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyFleet("fleet-nonexistent", 1, ""))
	})

	t.Run("create fleet with default type", func(t *testing.T) { //nolint:paralleltest // existing issue.
		f, _, err := b.CreateFleet(ec2.FleetCreateInput{TotalTargetCapacity: 1})
		require.NoError(t, err)
		assert.Equal(t, "maintain", f.FleetType)
		assert.Len(t, f.InstanceIDs, 1)
	})
}

// ---- Network Insights Path ----.
