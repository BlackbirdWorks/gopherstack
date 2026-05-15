package ec2_test

import (
	"testing"

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
