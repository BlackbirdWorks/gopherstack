package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestTagsCleanedUpOnDelete verifies that b.tags entries are removed when EC2
// resources are deleted, so deleted resources do not accumulate tags in memory
// forever. Terminated instances are handled separately by the janitor; see
// TestJanitor_TerminatedInstancesSweep.
func TestTagsCleanedUpOnDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn  func(t *testing.T, b *ec2.InMemoryBackend) (resourceID string)
		deleteFn func(b *ec2.InMemoryBackend, id string) error
		name     string
	}{
		{
			name: "DeleteSecurityGroup",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				sg, err := b.CreateSecurityGroup("test-sg", "test sg", "vpc-default")
				require.NoError(t, err)

				return sg.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteSecurityGroup(id)
			},
		},
		{
			name: "DeleteVpc",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				vpc, err := b.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)

				return vpc.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteVpc(id)
			},
		},
		{
			name: "DeleteSubnet",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				subnet, err := b.CreateSubnet("vpc-default", "10.1.0.0/24", "us-east-1a")
				require.NoError(t, err)

				return subnet.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteSubnet(id)
			},
		},
		{
			name: "DeleteVolume",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				vol, err := b.CreateVolume("us-east-1a", "gp2", 10)
				require.NoError(t, err)

				return vol.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteVolume(id)
			},
		},
		{
			name: "ReleaseAddress",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				addr, err := b.AllocateAddress()
				require.NoError(t, err)

				return addr.AllocationID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.ReleaseAddress(id)
			},
		},
		{
			name: "DeleteInternetGateway",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)

				return igw.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteInternetGateway(id)
			},
		},
		{
			name: "DeleteRouteTable",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)

				return rt.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteRouteTable(id)
			},
		},
		{
			name: "DeleteNatGateway",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				addr, err := b.AllocateAddress()
				require.NoError(t, err)

				ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
				require.NoError(t, err)

				return ngw.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteNatGateway(id)
			},
		},
		{
			name: "DeleteNetworkInterface",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				eni, err := b.CreateNetworkInterface("subnet-default", "test ENI")
				require.NoError(t, err)

				return eni.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteNetworkInterface(id)
			},
		},
		{
			name: "DeleteKeyPair",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				kp, err := b.CreateKeyPair("test-key")
				require.NoError(t, err)

				return kp.Name
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteKeyPair(id)
			},
		},
		{
			name: "DeletePlacementGroup",
			setupFn: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				pg, err := b.CreatePlacementGroup("test-pg", "cluster")
				require.NoError(t, err)

				return pg.Name
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeletePlacementGroup(id)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.setupFn(t, b)

			// Tag the resource.
			err := b.CreateTags([]string{id}, map[string]string{"key": "value"})
			require.NoError(t, err)

			// Confirm the tag exists.
			entries := b.DescribeTags([]string{id})
			assert.Len(t, entries, 1, "tag should exist before deletion")

			// Delete/terminate the resource.
			err = tt.deleteFn(b, id)
			require.NoError(t, err)

			// Tags must be removed immediately on delete.
			entries = b.DescribeTags([]string{id})
			assert.Empty(t, entries, "tags should be removed after deletion")
		})
	}
}

// TestJanitor_TerminatedInstancesSweep verifies that the EC2 janitor removes
// terminated instances and their tags once the TerminatedTTL has elapsed.
func TestJanitor_TerminatedInstancesSweep(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Launch and terminate an instance.
	insts, err := b.RunInstances("ami-test", "t2.micro", "", 1)
	require.NoError(t, err)
	instanceID := insts[0].ID

	_, err = b.TerminateInstances([]string{instanceID})
	require.NoError(t, err)

	// Tag the (now-terminated) instance.
	err = b.CreateTags([]string{instanceID}, map[string]string{"key": "value"})
	require.NoError(t, err)

	// Back-date TerminatedAt so it exceeds the TTL.
	b.SetInstanceTerminatedAtForTest(instanceID, time.Now().Add(-2*time.Hour))

	// Create a janitor with a 1-hour TTL (shorter than 2-hour offset).
	j := ec2.NewJanitor(b, time.Minute, time.Hour, 0)

	// Manually trigger the sweep.
	j.SweepTerminatedInstancesForTest(t.Context())

	// The instance must no longer appear in DescribeInstances.
	instances := b.DescribeInstances([]string{instanceID}, "")
	assert.Empty(t, instances, "terminated instance should be removed after janitor sweep")

	// The instance's tags must also be removed.
	entries := b.DescribeTags([]string{instanceID})
	assert.Empty(t, entries, "terminated instance tags should be removed after janitor sweep")
}

// TestJanitor_TerminatedInstancesNotSweptBeforeTTL verifies that terminated
// instances still within the TTL grace period are NOT removed by the janitor.
func TestJanitor_TerminatedInstancesNotSweptBeforeTTL(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Launch and terminate an instance.
	insts, err := b.RunInstances("ami-test", "t2.micro", "", 1)
	require.NoError(t, err)
	instanceID := insts[0].ID

	_, err = b.TerminateInstances([]string{instanceID})
	require.NoError(t, err)

	// TerminatedAt is set to now, which is within the 1-hour TTL.
	j := ec2.NewJanitor(b, time.Minute, time.Hour, 0)
	j.SweepTerminatedInstancesForTest(t.Context())

	// The instance must still appear in DescribeInstances (terminated state).
	instances := b.DescribeInstances([]string{instanceID}, "")
	require.Len(t, instances, 1, "terminated instance within TTL must stay visible")
	assert.Equal(t, "terminated", instances[0].State.Name)
}

// TestJanitor_CancelledSpotRequestsSweep verifies that the EC2 janitor removes
// cancelled spot requests and their tags once the CancelledSpotTTL has elapsed.
func TestJanitor_CancelledSpotRequestsSweep(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	req, err := b.RequestSpotInstances("ami-test", "t2.micro", "", "0.05")
	require.NoError(t, err)
	reqID := req.ID

	err = b.CancelSpotInstanceRequests([]string{reqID})
	require.NoError(t, err)

	// Tag the (now-cancelled) spot request.
	err = b.CreateTags([]string{reqID}, map[string]string{"env": "test"})
	require.NoError(t, err)

	// Back-date CancelledAt so it exceeds the TTL (7 hours > default 6 hours).
	b.SetSpotRequestCancelledAtForTest(reqID, time.Now().Add(-7*time.Hour))

	j := ec2.NewJanitor(b, time.Minute, time.Hour, 0)
	j.SweepCancelledSpotRequestsForTest(t.Context())

	reqs := b.DescribeSpotInstanceRequests([]string{reqID})
	assert.Empty(t, reqs, "cancelled spot request should be removed after TTL")

	entries := b.DescribeTags([]string{reqID})
	assert.Empty(t, entries, "cancelled spot request tags should be removed after TTL")
}

// TestJanitor_CancelledSpotRequestsNotSweptBeforeTTL verifies that recently
// cancelled spot requests are NOT removed before the TTL elapses.
func TestJanitor_CancelledSpotRequestsNotSweptBeforeTTL(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	req, err := b.RequestSpotInstances("ami-test", "t2.micro", "", "0.05")
	require.NoError(t, err)
	reqID := req.ID

	err = b.CancelSpotInstanceRequests([]string{reqID})
	require.NoError(t, err)

	// CancelledAt is now — within the 6-hour default TTL.
	j := ec2.NewJanitor(b, time.Minute, time.Hour, 0)
	j.SweepCancelledSpotRequestsForTest(t.Context())

	reqs := b.DescribeSpotInstanceRequests([]string{reqID})
	require.Len(t, reqs, 1, "spot request within TTL must stay visible")
	assert.Equal(t, "cancelled", reqs[0].State)
}

// TestTerminateInstances_ClosesAssociatedSpotRequest verifies that terminating
// the backing instance of an active spot request marks it "closed".
func TestTerminateInstances_ClosesAssociatedSpotRequest(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	req, err := b.RequestSpotInstances("ami-test", "t2.micro", "", "0.05")
	require.NoError(t, err)

	_, err = b.TerminateInstances([]string{req.InstanceID})
	require.NoError(t, err)

	reqs := b.DescribeSpotInstanceRequests([]string{req.ID})
	require.Len(t, reqs, 1)
	assert.Equal(t, "closed", reqs[0].State, "spot request must be closed when backing instance is terminated")
}
