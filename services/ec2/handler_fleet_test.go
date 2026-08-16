package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- EC2 Fleet ----.
func TestFleet(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var fleetID string

	t.Run("create fleet", func(t *testing.T) { //nolint:paralleltest // existing issue.
		f, err := b.CreateFleet("maintain", 5)
		require.NoError(t, err)
		assert.NotEmpty(t, f.FleetID)
		assert.Equal(t, "active", f.FleetState)
		assert.Equal(t, "maintain", f.FleetType)
		assert.Equal(t, 5, f.TotalTargetCapacity)
		fleetID = f.FleetID
	})

	t.Run("describe returns created fleet", func(t *testing.T) { //nolint:paralleltest // existing issue.
		fleets := b.DescribeFleets([]string{fleetID})
		require.Len(t, fleets, 1)
		assert.Equal(t, "active", fleets[0].FleetState)
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

	t.Run("delete fleet", func(t *testing.T) { //nolint:paralleltest // existing issue.
		deleted := b.DeleteFleets([]string{fleetID})
		require.Len(t, deleted, 1)
		assert.Equal(t, fleetID, deleted[0].FleetID)
		assert.Equal(t, "active", deleted[0].PreviousFleetState)
		fleets := b.DescribeFleets([]string{fleetID})
		assert.Empty(t, fleets)
	})

	t.Run("delete non-existent fleet returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		deleted := b.DeleteFleets([]string{"fleet-nonexistent"})
		assert.Empty(t, deleted)
	})

	t.Run("modify non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyFleet("fleet-nonexistent", 1, ""))
	})

	t.Run("create fleet with default type", func(t *testing.T) { //nolint:paralleltest // existing issue.
		f, err := b.CreateFleet("", 1)
		require.NoError(t, err)
		assert.Equal(t, "maintain", f.FleetType)
	})
}

// ---- Network Insights Path ----.
