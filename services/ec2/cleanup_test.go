package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestTagsCleanedUpOnDelete verifies that b.tags entries are removed when EC2
// resources are deleted, so terminated/deleted resources do not accumulate tags
// in memory forever.
func TestTagsCleanedUpOnDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn  func(b *ec2.InMemoryBackend) (resourceID string)
		deleteFn func(b *ec2.InMemoryBackend, id string) error
		name     string
	}{
		{
			name: "TerminateInstances",
			setupFn: func(b *ec2.InMemoryBackend) string {
				insts, err := b.RunInstances("ami-test", "t2.micro", "", 1)
				require.NoError(t, err)

				return insts[0].ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				_, err := b.TerminateInstances([]string{id})
				// After terminate, manually set TerminatedAt in the past so janitor sweeps it.
				// For this tag test we just need to verify tags are cleaned on sweep.
				return err
			},
		},
		{
			name: "DeleteSecurityGroup",
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
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
			setupFn: func(b *ec2.InMemoryBackend) string {
				eni, err := b.CreateNetworkInterface("subnet-default", "test ENI")
				require.NoError(t, err)

				return eni.ID
			},
			deleteFn: func(b *ec2.InMemoryBackend, id string) error {
				return b.DeleteNetworkInterface(id)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			id := tt.setupFn(b)

			// Tag the resource.
			err := b.CreateTags([]string{id}, map[string]string{"key": "value"})
			require.NoError(t, err)

			// Confirm the tag exists.
			entries := b.DescribeTags([]string{id})
			assert.Len(t, entries, 1, "tag should exist before deletion")

			// Delete/terminate the resource.
			err = tt.deleteFn(b, id)
			require.NoError(t, err)

			// For TerminateInstances, the instance is still visible (AWS TTL grace period),
			// but tags should remain until the janitor sweeps. Skip the tag-gone check here;
			// the janitor test covers the full sweep.
			if tt.name == "TerminateInstances" {
				return
			}

			// For all other resources, tags must be removed immediately on delete.
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
	j := ec2.NewJanitor(b, time.Minute, time.Hour)

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
	j := ec2.NewJanitor(b, time.Minute, time.Hour)
	j.SweepTerminatedInstancesForTest(t.Context())

	// The instance must still appear in DescribeInstances (terminated state).
	instances := b.DescribeInstances([]string{instanceID}, "")
	require.Len(t, instances, 1, "terminated instance within TTL must stay visible")
	assert.Equal(t, "terminated", instances[0].State.Name)
}
