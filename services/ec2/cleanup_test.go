package ec2_test

import (
	"slices"
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

				subnet, err := b.CreateSubnet("vpc-default", "172.31.16.0/24", "us-east-1a")
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

// TestTerminateInstances_DeletesAttachedENIs verifies that terminating an
// instance removes all ENIs attached to it, preventing ENI accumulation.
func TestTerminateInstances_DeletesAttachedENIs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		eniCount int
	}{
		{name: "single_eni", eniCount: 1},
		{name: "multiple_enis", eniCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			insts, err := b.RunInstances("ami-test", "t2.micro", "", 1)
			require.NoError(t, err)

			instanceID := insts[0].ID

			eniIDs := make([]string, tt.eniCount)
			for i := range tt.eniCount {
				eni, cerr := b.CreateNetworkInterface("subnet-default", "test-eni")
				require.NoError(t, cerr)

				_, aerr := b.AttachNetworkInterface(eni.ID, instanceID, i+1)
				require.NoError(t, aerr)

				eniIDs[i] = eni.ID
			}

			// Tag one ENI to verify the tag is also removed.
			err = b.CreateTags([]string{eniIDs[0]}, map[string]string{"Purpose": "test"})
			require.NoError(t, err)

			_, err = b.TerminateInstances([]string{instanceID})
			require.NoError(t, err)

			// All attached ENIs must be gone.
			for _, eniID := range eniIDs {
				enis := b.DescribeNetworkInterfaces([]string{eniID})
				assert.Empty(t, enis, "ENI %s should be deleted after instance termination", eniID)
			}

			// Tags for the first ENI must also be removed.
			entries := b.DescribeTags([]string{eniIDs[0]})
			assert.Empty(t, entries, "ENI tags should be removed when instance is terminated")
		})
	}
}

// TestTerminateInstances_OnlyDeletesAttachedENIs verifies that ENIs belonging to
// other instances are not affected when a specific instance is terminated.
func TestTerminateInstances_OnlyDeletesAttachedENIs(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	insts, err := b.RunInstances("ami-test", "t2.micro", "", 2)
	require.NoError(t, err)

	instanceA := insts[0].ID
	instanceB := insts[1].ID

	eniA, err := b.CreateNetworkInterface("subnet-default", "eni-for-A")
	require.NoError(t, err)

	eniB, err := b.CreateNetworkInterface("subnet-default", "eni-for-B")
	require.NoError(t, err)

	_, err = b.AttachNetworkInterface(eniA.ID, instanceA, 1)
	require.NoError(t, err)

	_, err = b.AttachNetworkInterface(eniB.ID, instanceB, 1)
	require.NoError(t, err)

	// Terminate only instance A.
	_, err = b.TerminateInstances([]string{instanceA})
	require.NoError(t, err)

	// ENI for A must be gone.
	assert.Empty(t, b.DescribeNetworkInterfaces([]string{eniA.ID}))

	// ENI for B must remain.
	enisB := b.DescribeNetworkInterfaces([]string{eniB.ID})
	require.Len(t, enisB, 1)
	assert.Equal(t, "in-use", enisB[0].Status)
}

// TestDeleteSubnet_CascadeDeletesENIs verifies that deleting a subnet removes
// all network interfaces associated with it.
func TestDeleteSubnet_CascadeDeletesENIs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		eniCount int
	}{
		{name: "no_enis", eniCount: 0},
		{name: "one_eni", eniCount: 1},
		{name: "multiple_enis", eniCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			subnet, err := b.CreateSubnet("vpc-default", "172.31.16.0/24", "us-east-1a")
			require.NoError(t, err)

			eniIDs := make([]string, tt.eniCount)
			for i := range tt.eniCount {
				eni, cerr := b.CreateNetworkInterface(subnet.ID, "test-eni")
				require.NoError(t, cerr)

				// Tag the ENI so we can verify tag cleanup.
				err = b.CreateTags([]string{eni.ID}, map[string]string{"k": "v"})
				require.NoError(t, err)

				eniIDs[i] = eni.ID
			}

			err = b.DeleteSubnet(subnet.ID)
			require.NoError(t, err)

			// All ENIs in the subnet must be removed.
			for _, eniID := range eniIDs {
				assert.Empty(t, b.DescribeNetworkInterfaces([]string{eniID}))
				assert.Empty(t, b.DescribeTags([]string{eniID}), "ENI tags must be removed with subnet")
			}

			// Subnet itself must be gone.
			subnets := b.DescribeSubnets([]string{subnet.ID})
			assert.Empty(t, subnets)
		})
	}
}

// TestDeleteSubnet_CascadeTerminatesInstances verifies that deleting a subnet
// marks all instances in that subnet as terminated.
func TestDeleteSubnet_CascadeTerminatesInstances(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	subnet, err := b.CreateSubnet("vpc-default", "172.31.16.0/24", "us-east-1a")
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-test", "t2.micro", subnet.ID, 2)
	require.NoError(t, err)

	err = b.DeleteSubnet(subnet.ID)
	require.NoError(t, err)

	for _, inst := range insts {
		remaining := b.DescribeInstances([]string{inst.ID}, "")
		require.Len(t, remaining, 1, "terminated instances should still be visible in grace period")
		assert.Equal(t, "terminated", remaining[0].State.Name,
			"instance %s should be terminated after subnet deletion", inst.ID)
	}
}

// TestDeleteVpc_CascadeDeletesDependents verifies that deleting a VPC removes
// all dependent resources: subnets, security groups, route tables, and ENIs.
func TestDeleteVpc_CascadeDeletesDependents(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	vpc, err := b.CreateVpc("10.99.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.99.1.0/24", "us-east-1a")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("test-sg", "test", vpc.ID)
	require.NoError(t, err)

	rt, err := b.CreateRouteTable(vpc.ID)
	require.NoError(t, err)

	eni, err := b.CreateNetworkInterface(subnet.ID, "test-eni")
	require.NoError(t, err)

	// Tag each resource so we verify tag cleanup.
	err = b.CreateTags([]string{subnet.ID, sg.ID, rt.ID, eni.ID}, map[string]string{"VPC": vpc.ID})
	require.NoError(t, err)

	err = b.DeleteVpc(vpc.ID)
	require.NoError(t, err)

	// VPC itself must be gone.
	assert.Empty(t, b.DescribeVpcs([]string{vpc.ID}))

	// All dependent resources must be removed.
	assert.Empty(t, b.DescribeSubnets([]string{subnet.ID}), "subnet must be removed")
	assert.Empty(t, b.DescribeSecurityGroups([]string{sg.ID}), "security group must be removed")
	assert.Empty(t, b.DescribeRouteTables([]string{rt.ID}), "route table must be removed")
	assert.Empty(t, b.DescribeNetworkInterfaces([]string{eni.ID}), "ENI must be removed")

	// Tags for all dependents must be removed.
	for _, resID := range []string{subnet.ID, sg.ID, rt.ID, eni.ID} {
		assert.Empty(t, b.DescribeTags([]string{resID}), "tags for %s must be removed", resID)
	}
}

// TestDeleteVpc_CascadeTerminatesInstances verifies that deleting a VPC marks
// all instances in that VPC as terminated.
func TestDeleteVpc_CascadeTerminatesInstances(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	vpc, err := b.CreateVpc("10.77.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.77.1.0/24", "us-east-1a")
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-test", "t2.micro", subnet.ID, 2)
	require.NoError(t, err)

	err = b.DeleteVpc(vpc.ID)
	require.NoError(t, err)

	for _, inst := range insts {
		remaining := b.DescribeInstances([]string{inst.ID}, "")
		require.Len(t, remaining, 1, "terminated instances should still be visible in grace period")
		assert.Equal(t, "terminated", remaining[0].State.Name,
			"instance %s should be terminated after VPC deletion", inst.ID)
	}
}

// TestUnassignPrivateIPAddresses_RecyclesIPs verifies that auto-allocated
// secondary IPs returned by UnassignPrivateIPAddresses are reused by the
// next allocPrivateIP call, preventing unbounded index growth.
func TestUnassignPrivateIPAddresses_RecyclesIPs(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	eni, err := b.CreateNetworkInterface("subnet-default", "test-eni")
	require.NoError(t, err)

	// Assign two secondary IPs by count (auto-allocated).
	err = b.AssignPrivateIPAddresses(eni.ID, 2, nil)
	require.NoError(t, err)

	enis := b.DescribeNetworkInterfaces([]string{eni.ID})
	require.Len(t, enis, 1)
	require.Len(t, enis[0].SecondaryPrivateIPs, 2, "expected 2 secondary IPs after assign")

	allocatedIPs := make([]string, len(enis[0].SecondaryPrivateIPs))
	copy(allocatedIPs, enis[0].SecondaryPrivateIPs)

	// Unassign one IP; it should be added to the free list.
	err = b.UnassignPrivateIPAddresses(eni.ID, []string{allocatedIPs[0]})
	require.NoError(t, err)

	// Assign a new IP by count – it must reuse the freed IP.
	err = b.AssignPrivateIPAddresses(eni.ID, 1, nil)
	require.NoError(t, err)

	enis = b.DescribeNetworkInterfaces([]string{eni.ID})
	require.Len(t, enis, 1)
	require.Len(t, enis[0].SecondaryPrivateIPs, 2, "expected 2 secondary IPs after reassign")

	// The freed IP must have been reused.
	assert.True(t,
		slices.Contains(enis[0].SecondaryPrivateIPs, allocatedIPs[0]),
		"freed IP %s should be reused by subsequent allocation", allocatedIPs[0],
	)
}

// TestCreateTags_NonExistentResourceReturnsError verifies that tagging a
// resource ID that does not exist returns an error.
func TestCreateTags_NonExistentResourceReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
	}{
		{name: "fake_instance", resourceID: "i-doesnotexist"},
		{name: "fake_vpc", resourceID: "vpc-doesnotexist"},
		{name: "fake_subnet", resourceID: "subnet-doesnotexist"},
		{name: "fake_sg", resourceID: "sg-doesnotexist"},
		{name: "fake_eni", resourceID: "eni-doesnotexist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			err := b.CreateTags([]string{tt.resourceID}, map[string]string{"Key": "Value"})
			require.Error(t, err, "CreateTags on non-existent resource must return an error")

			// No orphaned tag entries must be created.
			entries := b.DescribeTags([]string{tt.resourceID})
			assert.Empty(t, entries, "no tags should be stored for a non-existent resource")
		})
	}
}

// TestCreateTags_AtomicOnMixedResourceIDs verifies that CreateTags is atomic:
// if any resource ID in the list does not exist, no resources are tagged.
func TestCreateTags_AtomicOnMixedResourceIDs(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// "vpc-default" exists; "vpc-does-not-exist" does not.
	err := b.CreateTags(
		[]string{"vpc-default", "vpc-does-not-exist"},
		map[string]string{"Env": "test"},
	)
	require.Error(t, err, "CreateTags must fail when any resource ID is invalid")

	// The valid resource must NOT have been tagged (atomicity).
	entries := b.DescribeTags([]string{"vpc-default"})
	assert.Empty(t, entries, "vpc-default must not be tagged when CreateTags fails atomically")
}

// TestJanitor_DefensiveENISweep verifies that the janitor removes orphaned ENIs
// that are still referencing a terminated instance (e.g. state restored from a
// snapshot that predates the ENI cleanup fix).
func TestJanitor_DefensiveENISweep(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Launch and terminate an instance normally.
	insts, err := b.RunInstances("ami-test", "t2.micro", "", 1)
	require.NoError(t, err)

	instanceID := insts[0].ID

	_, err = b.TerminateInstances([]string{instanceID})
	require.NoError(t, err)

	// Inject an orphaned ENI (simulating a snapshot restore before the fix).
	orphan := &ec2.NetworkInterface{
		ID:         "eni-orphan-test",
		InstanceID: instanceID,
		SubnetID:   "subnet-default",
		VPCID:      "vpc-default",
		PrivateIP:  "172.31.100.1",
		Status:     "in-use",
	}
	b.InjectOrphanedENIForTest(orphan)

	// Confirm the orphaned ENI is present.
	enis := b.DescribeNetworkInterfaces([]string{orphan.ID})
	require.Len(t, enis, 1, "orphaned ENI should be present before janitor sweep")

	// Back-date TerminatedAt to exceed the TTL.
	b.SetInstanceTerminatedAtForTest(instanceID, time.Now().Add(-2*time.Hour))

	j := ec2.NewJanitor(b, time.Minute, time.Hour, 0)
	j.SweepTerminatedInstancesForTest(t.Context())

	// The orphaned ENI must be removed by the janitor.
	enis = b.DescribeNetworkInterfaces([]string{orphan.ID})
	assert.Empty(t, enis, "orphaned ENI should be removed by janitor defensive sweep")
}

// TestTerminateInstances_DetachesVolumesAndEIPs verifies that when an instance
// is terminated its EBS volume attachments are cleared and its Elastic IP
// associations are removed, preventing zombie attachments/associations.
func TestTerminateInstances_DetachesVolumesAndEIPs(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	insts, err := b.RunInstances("ami-test", "t2.micro", "", 1)
	require.NoError(t, err)

	instanceID := insts[0].ID

	// Attach a volume to the instance.
	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)

	_, err = b.AttachVolume(vol.ID, instanceID, "/dev/sdf")
	require.NoError(t, err)

	// Associate an EIP with the instance.
	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	_, err = b.AssociateAddress(addr.AllocationID, instanceID)
	require.NoError(t, err)

	// Terminate the instance.
	_, err = b.TerminateInstances([]string{instanceID})
	require.NoError(t, err)

	// Volume must be detached (available).
	vols := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, vols, 1)
	assert.Equal(t, "available", vols[0].State, "volume must be detached after instance termination")
	assert.Nil(t, vols[0].Attachment, "volume attachment must be nil after instance termination")

	// EIP must be disassociated.
	addrs := b.DescribeAddresses([]string{addr.AllocationID})
	require.Len(t, addrs, 1)
	assert.Empty(t, addrs[0].InstanceID, "EIP instance association must be cleared after termination")
	assert.Empty(t, addrs[0].AssociationID, "EIP association ID must be cleared after termination")
}

// TestDeleteNetworkInterface_RecyclesPrivateIP verifies that explicitly
// deleting an unattached ENI returns its private IP to the free list so it
// can be reused by future allocations.
func TestDeleteNetworkInterface_RecyclesPrivateIP(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	eni, err := b.CreateNetworkInterface("subnet-default", "test-eni")
	require.NoError(t, err)

	privateIP := eni.PrivateIP

	err = b.DeleteNetworkInterface(eni.ID)
	require.NoError(t, err)

	// The next allocation should reuse the recycled IP.
	eni2, err := b.CreateNetworkInterface("subnet-default", "test-eni-2")
	require.NoError(t, err)

	assert.Equal(t, privateIP, eni2.PrivateIP, "deleted ENI's private IP must be reused")
}

// TestDeleteNatGateway_RecyclesPrivateIP verifies that deleting a NAT gateway
// returns its private IP to the free list.
func TestDeleteNatGateway_RecyclesPrivateIP(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)

	ngwPrivateIP := ngw.PrivateIP

	err = b.DeleteNatGateway(ngw.ID)
	require.NoError(t, err)

	// The next allocation must reuse the recycled IP.
	eni, err := b.CreateNetworkInterface("subnet-default", "test-eni")
	require.NoError(t, err)

	assert.Equal(t, ngwPrivateIP, eni.PrivateIP, "deleted NAT GW's private IP must be reused")
}

// TestDeleteSubnet_CascadeDeletesNatGateways verifies that deleting a subnet
// cascade-deletes any NAT gateways in that subnet.
func TestDeleteSubnet_CascadeDeletesNatGateways(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	subnet, err := b.CreateSubnet("vpc-default", "172.31.16.0/24", "us-east-1a")
	require.NoError(t, err)

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	ngw, err := b.CreateNatGateway(subnet.ID, addr.AllocationID)
	require.NoError(t, err)

	err = b.DeleteSubnet(subnet.ID)
	require.NoError(t, err)

	ngws := b.DescribeNatGateways([]string{ngw.ID})
	assert.Empty(t, ngws, "NAT gateway must be deleted when its subnet is deleted")
}

// TestDeleteVpc_CascadeDeletesIGWsAndNatGateways verifies that deleting a VPC
// cascade-deletes internet gateways attached to it and NAT gateways in its
// subnets.
func TestDeleteVpc_CascadeDeletesIGWsAndNatGateways(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	vpc, err := b.CreateVpc("10.88.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.88.1.0/24", "us-east-1a")
	require.NoError(t, err)

	// Create and attach an IGW to the VPC.
	igw, err := b.CreateInternetGateway()
	require.NoError(t, err)

	err = b.AttachInternetGateway(igw.ID, vpc.ID)
	require.NoError(t, err)

	// Create a NAT gateway in the VPC's subnet.
	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	ngw, err := b.CreateNatGateway(subnet.ID, addr.AllocationID)
	require.NoError(t, err)

	err = b.DeleteVpc(vpc.ID)
	require.NoError(t, err)

	assert.Empty(t, b.DescribeInternetGateways([]string{igw.ID}),
		"internet gateway must be deleted when its VPC is deleted")
	assert.Empty(t, b.DescribeNatGateways([]string{ngw.ID}),
		"NAT gateway must be deleted when its VPC is deleted")
}

// TestDeleteVpc_CascadeDetachesVolumesAndEIPs verifies that when a VPC is
// deleted the volumes attached to its instances are detached and any Elastic
// IPs associated with those instances are disassociated.
func TestDeleteVpc_CascadeDetachesVolumesAndEIPs(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	vpc, err := b.CreateVpc("10.55.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.55.1.0/24", "us-east-1a")
	require.NoError(t, err)

	insts, err := b.RunInstances("ami-test", "t2.micro", subnet.ID, 1)
	require.NoError(t, err)

	instanceID := insts[0].ID

	vol, err := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, err)

	_, err = b.AttachVolume(vol.ID, instanceID, "/dev/sdf")
	require.NoError(t, err)

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	_, err = b.AssociateAddress(addr.AllocationID, instanceID)
	require.NoError(t, err)

	err = b.DeleteVpc(vpc.ID)
	require.NoError(t, err)

	vols := b.DescribeVolumes([]string{vol.ID})
	require.Len(t, vols, 1)
	assert.Equal(t, "available", vols[0].State, "volume must be detached when VPC is deleted")

	addrs := b.DescribeAddresses([]string{addr.AllocationID})
	require.Len(t, addrs, 1)
	assert.Empty(t, addrs[0].InstanceID, "EIP must be disassociated when VPC is deleted")
}
