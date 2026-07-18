package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func newSpotFleetBackend() *ec2.InMemoryBackend {
	return ec2.NewInMemoryBackend("123456789012", "us-east-1")
}

func validSpotFleetConfig() ec2.SpotFleetRequestConfig {
	return ec2.SpotFleetRequestConfig{
		TargetCapacity: 3,
		LaunchSpecifications: []ec2.SpotFleetLaunchSpecification{
			{
				ImageID:          "ami-12345678",
				InstanceType:     "t3.medium",
				WeightedCapacity: 1.0,
			},
		},
	}
}

// ---- RequestSpotFleet ----

func TestRequestSpotFleet_Success(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)
	assert.NotEmpty(t, fleet.SpotFleetRequestID)
	assert.Greater(t, len(fleet.SpotFleetRequestID), 4)
	assert.Equal(t, ec2.SpotFleetStateActive, fleet.SpotFleetRequestState)
	assert.Equal(t, ec2.SpotFleetActivityFulfilled, fleet.ActivityStatus)
	assert.Equal(t, 3, fleet.SpotFleetRequestConfig.TargetCapacity)
	assert.Len(t, fleet.InstanceIDs, 3)
	assert.InDelta(t, 3.0, fleet.FulfilledCapacity, 0.01)
}

func TestRequestSpotFleet_DefaultAllocationStrategy(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)
	assert.Equal(
		t,
		ec2.SpotFleetAllocationStrategyLowestPrice,
		fleet.SpotFleetRequestConfig.AllocationStrategy,
	)
}

func TestRequestSpotFleet_WeightedCapacity(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	config := ec2.SpotFleetRequestConfig{
		TargetCapacity: 4,
		LaunchSpecifications: []ec2.SpotFleetLaunchSpecification{
			{
				ImageID:          "ami-12345678",
				InstanceType:     "c5.2xlarge",
				WeightedCapacity: 2.0,
			},
		},
	}
	fleet, err := b.RequestSpotFleet(config)
	require.NoError(t, err)
	// With weight=2.0 and target=4, we should get 2 instances.
	assert.Len(t, fleet.InstanceIDs, 2)
	assert.InDelta(t, 4.0, fleet.FulfilledCapacity, 0.01)
}

func TestRequestSpotFleet_ZeroCapacity(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	config := validSpotFleetConfig()
	config.TargetCapacity = 0
	fleet, err := b.RequestSpotFleet(config)
	require.NoError(t, err)
	assert.Empty(t, fleet.InstanceIDs)
}

func TestRequestSpotFleet_NegativeCapacity(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	config := validSpotFleetConfig()
	config.TargetCapacity = -1
	_, err := b.RequestSpotFleet(config)
	require.Error(t, err)
}

func TestRequestSpotFleet_NoLaunchSpecs(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	config := ec2.SpotFleetRequestConfig{
		TargetCapacity:       2,
		LaunchSpecifications: nil,
	}
	_, err := b.RequestSpotFleet(config)
	require.Error(t, err)
}

func TestRequestSpotFleet_InstancesRunning(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	// Verify the instances were created and are running.
	instances := b.DescribeInstances(fleet.InstanceIDs, "")
	assert.Len(t, instances, 3)

	for _, inst := range instances {
		assert.Equal(t, "running", inst.State.Name)
	}
}

// ---- DescribeSpotFleetRequests ----

func TestDescribeSpotFleetRequests_All(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	_, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)
	_, err = b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	fleets, err := b.DescribeSpotFleetRequests(nil)
	require.NoError(t, err)
	assert.Len(t, fleets, 2)
}

func TestDescribeSpotFleetRequests_ByID(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet1, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)
	_, err = b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	fleets, err := b.DescribeSpotFleetRequests([]string{fleet1.SpotFleetRequestID})
	require.NoError(t, err)
	assert.Len(t, fleets, 1)
	assert.Equal(t, fleet1.SpotFleetRequestID, fleets[0].SpotFleetRequestID)
}

func TestDescribeSpotFleetRequests_NotFound(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	_, err := b.DescribeSpotFleetRequests([]string{"sfr-doesnotexist"})
	require.Error(t, err)
}

// ---- CancelSpotFleetRequests ----

func TestCancelSpotFleetRequests_NoTerminate(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	results, err := b.CancelSpotFleetRequests([]string{fleet.SpotFleetRequestID}, false)
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, ec2.SpotFleetStateCancelling, results[0].CurrentSpotFleetRequestState)
	assert.Equal(t, ec2.SpotFleetStateActive, results[0].PreviousSpotFleetRequestState)

	// Instances should still be running.
	instances := b.DescribeInstances(fleet.InstanceIDs, "")
	for _, inst := range instances {
		assert.Equal(t, "running", inst.State.Name)
	}
}

func TestCancelSpotFleetRequests_WithTerminate(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)
	instanceIDs := fleet.InstanceIDs

	results, err := b.CancelSpotFleetRequests([]string{fleet.SpotFleetRequestID}, true)
	require.NoError(t, err)
	assert.Equal(t, ec2.SpotFleetStateCancelled, results[0].CurrentSpotFleetRequestState)

	// Instances should be terminated.
	instances := b.DescribeInstances(instanceIDs, "")
	for _, inst := range instances {
		assert.Equal(t, "terminated", inst.State.Name)
	}
}

func TestCancelSpotFleetRequests_Empty(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	_, err := b.CancelSpotFleetRequests(nil, false)
	require.Error(t, err)
}

// ---- ModifySpotFleetRequest ----

func TestModifySpotFleetRequest_ScaleUp(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)
	assert.Len(t, fleet.InstanceIDs, 3)

	updated, err := b.ModifySpotFleetRequest(fleet.SpotFleetRequestID, 5, "")
	require.NoError(t, err)
	assert.Equal(t, 5, updated.SpotFleetRequestConfig.TargetCapacity)
	assert.Len(t, updated.InstanceIDs, 5)
}

func TestModifySpotFleetRequest_ScaleDown(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	updated, err := b.ModifySpotFleetRequest(
		fleet.SpotFleetRequestID,
		1,
		ec2.SpotFleetTerminationDefault,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, updated.SpotFleetRequestConfig.TargetCapacity)
	assert.Len(t, updated.InstanceIDs, 1)
}

func TestModifySpotFleetRequest_NotFound(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	_, err := b.ModifySpotFleetRequest("sfr-doesnotexist", 5, "")
	require.Error(t, err)
}

func TestModifySpotFleetRequest_NotActive(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	// Cancel first.
	_, err = b.CancelSpotFleetRequests([]string{fleet.SpotFleetRequestID}, true)
	require.NoError(t, err)

	_, err = b.ModifySpotFleetRequest(fleet.SpotFleetRequestID, 5, "")
	require.Error(t, err)
}

// ---- DescribeSpotFleetInstances ----

func TestDescribeSpotFleetInstances(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	instances, err := b.DescribeSpotFleetInstances(fleet.SpotFleetRequestID)
	require.NoError(t, err)
	assert.Len(t, instances, 3)

	for _, inst := range instances {
		assert.NotEmpty(t, inst.InstanceID)
		assert.NotEmpty(t, inst.InstanceType)
		assert.Equal(t, "healthy", inst.InstanceHealth)
	}
}

func TestDescribeSpotFleetInstances_NotFound(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	_, err := b.DescribeSpotFleetInstances("sfr-doesnotexist")
	require.Error(t, err)
}

// ---- DescribeSpotFleetRequestHistory ----

func TestDescribeSpotFleetRequestHistory(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	records, err := b.DescribeSpotFleetRequestHistory(
		fleet.SpotFleetRequestID,
		fleet.CreateTime.Add(-1),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, records)
	assert.Equal(t, "fleetRequestChange", records[0].EventType)
}

func TestDescribeSpotFleetRequestHistory_Empty_After_Time(t *testing.T) {
	t.Parallel()
	b := newSpotFleetBackend()
	fleet, err := b.RequestSpotFleet(validSpotFleetConfig())
	require.NoError(t, err)

	// Request history from far in the future.
	records, err := b.DescribeSpotFleetRequestHistory(
		fleet.SpotFleetRequestID,
		fleet.CreateTime.Add(999999),
	)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestSpotPriceHistory_Deterministic(t *testing.T) {
	t.Parallel()

	// use deterministic time since history drops records older than 24h
	now := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC).UTC().Add(-24 * time.Hour)

	records1 := ec2.GenerateSpotPriceHistory(
		[]string{"t3.micro"},
		[]string{"us-east-1a"},
		[]string{"Linux/UNIX"},
		now,
		"us-east-1",
	)

	records2 := ec2.GenerateSpotPriceHistory(
		[]string{"t3.micro"},
		[]string{"us-east-1a"},
		[]string{"Linux/UNIX"},
		now,
		"us-east-1",
	)

	require.NotEmpty(t, records1)
	require.Len(t, records1, len(records2))
	assert.Equal(t, records1[0].SpotPrice, records2[0].SpotPrice, "price must be deterministic")
}

func TestSpotPriceHistory_DefaultsPopulated(t *testing.T) {
	t.Parallel()

	records := ec2.GenerateSpotPriceHistory(nil, nil, nil, time.Time{}, "us-east-1")
	assert.NotEmpty(t, records, "default instance types should produce records")
}

// ---- Optimization: CIDR overlap ----

func TestGetSpotPlacementScores(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	scores, err := b.GetSpotPlacementScores([]string{"m5.large", "m5.xlarge"}, nil, false)
	require.NoError(t, err)
	require.NotEmpty(t, scores)

	for _, s := range scores {
		assert.NotEmpty(t, s.Region)
		assert.Empty(t, s.AvailabilityZoneID)
		assert.GreaterOrEqual(t, s.Score, int32(1))
		assert.LessOrEqual(t, s.Score, int32(10))
	}

	// Deterministic: same inputs produce the same scores.
	again, err := b.GetSpotPlacementScores([]string{"m5.large", "m5.xlarge"}, nil, false)
	require.NoError(t, err)
	assert.Equal(t, scores, again)

	azScores, err := b.GetSpotPlacementScores([]string{"m5.large"}, []string{"us-east-1"}, true)
	require.NoError(t, err)
	require.NotEmpty(t, azScores)

	for _, s := range azScores {
		assert.Empty(t, s.Region)
		assert.NotEmpty(t, s.AvailabilityZoneID)
	}

	_, err = b.GetSpotPlacementScores(nil, nil, false)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestSpotFleetHistory_Capped(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	config := ec2.SpotFleetRequestConfig{
		SpotPrice:      "0.05",
		TargetCapacity: 1,
		LaunchSpecifications: []ec2.SpotFleetLaunchSpecification{
			{ImageID: "ami-123", InstanceType: "t3.micro"},
		},
	}

	fleet, err := b.RequestSpotFleet(config)
	require.NoError(t, err)

	// Trigger many history records via ModifySpotFleetRequest.
	for i := range 600 {
		newCap := 1 + (i % 3)
		_, modErr := b.ModifySpotFleetRequest(fleet.SpotFleetRequestID, newCap, "")
		require.NoError(t, modErr)
	}

	// History should be capped.
	history, err := b.DescribeSpotFleetRequestHistory(fleet.SpotFleetRequestID, time.Time{})
	require.NoError(t, err)
	assert.LessOrEqual(
		t,
		len(history),
		500,
		"history should be capped at maxSpotFleetHistoryEntries",
	)
}

// ---- helpers ----
