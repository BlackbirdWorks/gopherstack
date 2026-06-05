package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestEC2Lifecycle_RunInstances_StartsPending verifies RunInstances creates
// instances in the pending state (AWS state machine: pending → running).
func TestEC2Lifecycle_RunInstances_StartsPending(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	assert.Equal(t, "pending", instances[0].State.Name, "newly launched instance must be pending")
	assert.Equal(t, 0, instances[0].State.Code)
}

// TestEC2Lifecycle_ReconcilerAdvancesPendingToRunning verifies the reconciler
// moves pending instances to running.
func TestEC2Lifecycle_ReconcilerAdvancesPendingToRunning(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t2.micro", "", 2)
	require.NoError(t, err)

	// Before tick, all pending.
	for _, inst := range instances {
		running := b.DescribeInstances([]string{inst.ID}, "")
		require.Len(t, running, 1)
		assert.Equal(t, "pending", running[0].State.Name)
	}

	b.TickLifecycleForTest() // pending → running

	// After tick, all running.
	for _, inst := range instances {
		running := b.DescribeInstances([]string{inst.ID}, "")
		require.Len(t, running, 1)
		assert.Equal(t, "running", running[0].State.Name)
	}
}

// TestEC2Lifecycle_StopInstances_ReturnsStopping verifies StopInstances returns
// the intermediate stopping state (AWS state machine: running → stopping → stopped).
func TestEC2Lifecycle_StopInstances_ReturnsStopping(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running

	changes, err := b.StopInstances([]string{instances[0].ID})
	require.NoError(t, err)
	require.Len(t, changes, 1)

	assert.Equal(t, "running", changes[0].PreviousState.Name)
	assert.Equal(t, "stopping", changes[0].CurrentState.Name, "StopInstances must return stopping")

	// Backend state is stopping until reconciler runs.
	all := b.DescribeInstances([]string{instances[0].ID}, "")
	require.Len(t, all, 1)
	assert.Equal(t, "stopping", all[0].State.Name)

	b.TickLifecycleForTest() // stopping → stopped

	all = b.DescribeInstances([]string{instances[0].ID}, "")
	require.Len(t, all, 1)
	assert.Equal(t, "stopped", all[0].State.Name)
}

// TestEC2Lifecycle_StartInstances_ReturnsPending verifies StartInstances returns
// the intermediate pending state (AWS state machine: stopped → pending → running).
func TestEC2Lifecycle_StartInstances_ReturnsPending(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	b.TickLifecycleForTest() // pending → running
	_, err = b.StopInstances([]string{instances[0].ID})
	require.NoError(t, err)
	b.TickLifecycleForTest() // stopping → stopped

	changes, err := b.StartInstances([]string{instances[0].ID})
	require.NoError(t, err)
	require.Len(t, changes, 1)

	assert.Equal(t, "stopped", changes[0].PreviousState.Name)
	assert.Equal(t, "pending", changes[0].CurrentState.Name, "StartInstances must return pending")

	b.TickLifecycleForTest() // pending → running

	all := b.DescribeInstances([]string{instances[0].ID}, "")
	require.Len(t, all, 1)
	assert.Equal(t, "running", all[0].State.Name)
}

// TestEC2Lifecycle_TerminateInstances_ReturnsShuttingDown verifies TerminateInstances
// returns the intermediate shutting-down state.
func TestEC2Lifecycle_TerminateInstances_ReturnsShuttingDown(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)

	changes, err := b.TerminateInstances([]string{instances[0].ID})
	require.NoError(t, err)
	require.Len(t, changes, 1)

	assert.Equal(t, "shutting-down", changes[0].CurrentState.Name, "TerminateInstances must return shutting-down")

	// Backend state is shutting-down until reconciler runs.
	all := b.DescribeInstances([]string{instances[0].ID}, "")
	require.Len(t, all, 1)
	assert.Equal(t, "shutting-down", all[0].State.Name)

	b.TickLifecycleForTest() // shutting-down → terminated

	all = b.DescribeInstances([]string{instances[0].ID}, "")
	require.Len(t, all, 1)
	assert.Equal(t, "terminated", all[0].State.Name)
}

// TestEC2Lifecycle_StopPendingInstance verifies AWS-compatible behavior:
// stopping a pending instance is allowed.
func TestEC2Lifecycle_StopPendingInstance(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)
	// Instance is pending — stop it before reconciler advances to running.

	changes, err := b.StopInstances([]string{instances[0].ID})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "stopping", changes[0].CurrentState.Name)
}

// TestEC2Lifecycle_BackgroundReconciler verifies the background goroutine
// advances pending instances to running within a short time window.
func TestEC2Lifecycle_BackgroundReconciler(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	instances, err := b.RunInstances("ami-123", "t2.micro", "", 1)
	require.NoError(t, err)

	// Wait up to 500ms for the goroutine to advance state.
	deadline := time.Now().Add(500 * time.Millisecond)

	for time.Now().Before(deadline) {
		all := b.DescribeInstances([]string{instances[0].ID}, "")
		require.Len(t, all, 1)

		if all[0].State.Name == "running" {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("instance did not advance from pending to running within 500ms")
}
