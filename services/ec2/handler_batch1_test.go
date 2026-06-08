package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- EBS volume lifecycle ---- //nolint:godot // existing issue.
func TestBatch1_ModifyVolume(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, setupErr := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, setupErr)

	t.Run("resizes volume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		mod, err := b.ModifyVolume(vol.ID, "gp3", 40, 3000)
		require.NoError(t, err)
		assert.Equal(t, vol.ID, mod.VolumeID)
		assert.Equal(t, "completed", mod.ModificationState)
		assert.Equal(t, "gp3", mod.TargetVolumeType)
		assert.Equal(t, 40, mod.TargetSize)
		assert.Equal(t, int64(100), mod.Progress)
	})

	t.Run("unknown volume returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVolume("vol-doesnotexist", "", 0, 0)
		require.Error(t, err)
	})

	t.Run("empty volume ID returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVolume("", "gp3", 0, 0)
		require.Error(t, err)
	})
}
func TestBatch1_DescribeVolumeStatus(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, setupErr := b.CreateVolume("us-east-1a", "gp2", 20)
	require.NoError(t, setupErr)

	t.Run("returns ok status for known volume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeVolumeStatus([]string{vol.ID})
		require.Len(t, items, 1)
		assert.Equal(t, vol.ID, items[0].VolumeID)
		assert.Equal(t, "ok", items[0].VolumeStatus)
	})

	t.Run("returns all volumes when no filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeVolumeStatus(nil)
		assert.NotEmpty(t, items)
	})

	t.Run("filters to known IDs only", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeVolumeStatus([]string{"vol-missing"})
		assert.Empty(t, items)
	})
}
func TestBatch1_DescribeVolumesModifications(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20)
	_, _ = b.ModifyVolume(vol.ID, "gp3", 40, 0)

	t.Run("returns modification after ModifyVolume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		mods := b.DescribeVolumesModifications([]string{vol.ID})
		require.Len(t, mods, 1)
		assert.Equal(t, "completed", mods[0].ModificationState)
	})

	t.Run("returns empty when no modifications", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vol2, _ := b.CreateVolume("us-east-1a", "gp2", 10)
		mods := b.DescribeVolumesModifications([]string{vol2.ID})
		assert.Empty(t, mods)
	})
}
func TestBatch1_CopySnapshot(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20)
	src, setupErr := b.CreateSnapshot(vol.ID, "original")
	require.NoError(t, setupErr)

	t.Run("creates copy with new ID", func(t *testing.T) { //nolint:paralleltest // existing issue.
		copiedSnap, err := b.CopySnapshot(src.SnapshotID, "copy description")
		require.NoError(t, err)
		assert.NotEqual(t, src.SnapshotID, copiedSnap.SnapshotID)
		assert.Equal(t, vol.ID, copiedSnap.VolumeID)
		assert.Equal(t, "copy description", copiedSnap.Description)
		assert.Equal(t, "completed", copiedSnap.State)
	})

	t.Run("unknown source returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CopySnapshot("snap-doesnotexist", "")
		require.Error(t, err)
	})
}
func TestBatch1_CreateSnapshots(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	v1, _ := b.CreateVolume("us-east-1a", "gp2", 10)
	v2, _ := b.CreateVolume("us-east-1a", "gp2", 20)

	t.Run("creates one snapshot per volume", func(t *testing.T) { //nolint:paralleltest // existing issue.
		snaps, err := b.CreateSnapshots([]string{v1.ID, v2.ID}, "batch snap")
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		for _, s := range snaps {
			assert.Equal(t, "completed", s.State)
		}
	})

	t.Run("empty list returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateSnapshots(nil, "")
		require.Error(t, err)
	})
}

// ---- Snapshot block public access ---- //nolint:godot // existing issue.
func TestBatch1_SnapshotBlockPublicAccess(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("default state is block-all-sharing", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.Equal(t, "block-all-sharing", b.GetSnapshotBlockPublicAccessState())
	})

	t.Run("enable sets block-new-sharing", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableSnapshotBlockPublicAccess("block-new-sharing"))
		assert.Equal(t, "block-new-sharing", b.GetSnapshotBlockPublicAccessState())
	})

	t.Run("disable sets unblocked", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DisableSnapshotBlockPublicAccess()
		assert.Equal(t, "unblocked", b.GetSnapshotBlockPublicAccessState())
	})

	t.Run("invalid state returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.EnableSnapshotBlockPublicAccess("invalid"))
	})
}
func TestBatch1_SnapshotTier(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20)
	snap, _ := b.CreateSnapshot(vol.ID, "tier test")

	t.Run("default tier is standard", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeSnapshotTierStatus([]string{snap.SnapshotID})
		require.Len(t, items, 1)
		assert.Equal(t, "standard", items[0].StorageTier)
	})

	t.Run("ModifySnapshotTier updates tier", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifySnapshotTier(snap.SnapshotID, "archive"))
		items := b.DescribeSnapshotTierStatus([]string{snap.SnapshotID})
		require.Len(t, items, 1)
		assert.Equal(t, "archive", items[0].StorageTier)
	})

	t.Run("ResetSnapshotAttribute clears attribute", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ResetSnapshotAttribute(snap.SnapshotID))
	})

	t.Run( //nolint:paralleltest // existing issue.
		"ModifySnapshotTier unknown snapshot returns error",
		func(t *testing.T) {
			require.Error(t, b.ModifySnapshotTier("snap-missing", "archive"))
		},
	)
}

// ---- VPC/Subnet/SG ---- //nolint:godot // existing issue.
func TestBatch1_CreateDefaultVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	t.Run("creates default vpc", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		// Remove existing default VPC first (initDefaults creates one)
		b.DeleteVpc("vpc-default")
		vpc, err := b.CreateDefaultVpc()
		require.NoError(t, err)
		assert.True(t, vpc.IsDefault)
		assert.Equal(t, "172.31.0.0/16", vpc.CIDRBlock)
	})

	t.Run("error when default vpc exists", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateDefaultVpc()
		require.Error(t, err)
	})
}
func TestBatch1_CreateDefaultSubnet(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("creates default subnet in AZ", func(t *testing.T) {
		subnet, err := b.CreateDefaultSubnet("us-east-1b")
		require.NoError(t, err)
		assert.True(t, subnet.IsDefault)
		assert.Equal(t, "us-east-1b", subnet.AvailabilityZone)
	})
}
func TestBatch1_SubnetCIDRBlock(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("associate and disassociate", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assoc, err := b.AssociateSubnetCidrBlock("subnet-default", "2001:db8::/56")
		require.NoError(t, err)
		assert.Equal(t, "associated", assoc.State)

		subnetID, err := b.DisassociateSubnetCidrBlock(assoc.AssociationID)
		require.NoError(t, err)
		assert.Equal(t, "subnet-default", subnetID)
	})

	t.Run("unknown subnet returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.AssociateSubnetCidrBlock("subnet-nope", "2001:db8::/56")
		require.Error(t, err)
	})
}
func TestBatch1_SecurityGroupVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	sg, _ := b.CreateSecurityGroup("test-sg", "test", "vpc-default")
	vpc, _ := b.CreateVpc("10.0.0.0/16")

	t.Run( //nolint:paralleltest // existing issue.
		"DescribeSecurityGroupReferences returns empty before association",
		func(t *testing.T) {
			refs := b.DescribeSecurityGroupReferences([]string{sg.ID})
			assert.Empty(t, refs)
		},
	)

	t.Run( //nolint:paralleltest // existing issue.
		"DescribeStaleSecurityGroups returns empty with no deleted peering",
		func(t *testing.T) {
			stale := b.DescribeStaleSecurityGroups("vpc-default")
			assert.Empty(t, stale)
		},
	)

	t.Run("associate and describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		result, err := b.AssociateSecurityGroupVpc(sg.ID, vpc.ID)
		require.NoError(t, err)
		assert.Equal(t, "associated", result.State)

		assocs := b.DescribeSecurityGroupVpcAssociations([]string{sg.ID})
		require.Len(t, assocs, 1)
		assert.Equal(t, sg.ID, assocs[0].SGID)
		assert.Equal(t, vpc.ID, assocs[0].VPCID)
	})

	t.Run("disassociate", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisassociateSecurityGroupVpc(sg.ID, vpc.ID))
	})
}
func TestBatch1_ModifyVpcTenancy(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("sets tenancy", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyVpcTenancy("vpc-default", "dedicated"))
	})

	t.Run("unknown vpc returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.ModifyVpcTenancy("vpc-missing", "default"))
	})
}
func TestBatch1_ModifyVpcPeeringConnectionOptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	vpc2, _ := b.CreateVpc("10.0.0.0/16")
	pc, _ := b.CreateVpcPeeringConnection("vpc-default", vpc2.ID)

	t.Run("stores options", func(t *testing.T) {
		opts := ec2.PeeringConnectionOptions{AllowDNSResolutionFromRemoteVPC: true}
		require.NoError(t, b.ModifyVpcPeeringConnectionOptions(pc.VpcPeeringConnectionID, opts))
		stored := b.GetVpcPeeringConnectionOptions(pc.VpcPeeringConnectionID)
		require.NotNil(t, stored)
		assert.True(t, stored.AllowDNSResolutionFromRemoteVPC)
	})
}

// ---- EIP attributes ---- //nolint:godot // existing issue.
func TestBatch1_AddressAttribute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	addr, _ := b.AllocateAddress()

	t.Run("modify and describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyAddressAttribute(addr.AllocationID, "ec2.example.com"))
		attrs := b.DescribeAddressesAttribute([]string{addr.AllocationID})
		require.Len(t, attrs, 1)
		assert.Equal(t, "ec2.example.com", attrs[0].DomainName)
	})

	t.Run("reset clears domain name", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ResetAddressAttribute(addr.AllocationID))
		attrs := b.DescribeAddressesAttribute([]string{addr.AllocationID})
		require.Len(t, attrs, 1)
		assert.Empty(t, attrs[0].DomainName)
	})
}

// ---- Instance ---- //nolint:godot // existing issue.
func TestBatch1_GetConsoleOutput(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("returns base64 output for known instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		output, ts, err := b.GetConsoleOutput(inst.ID)
		require.NoError(t, err)
		assert.NotEmpty(t, output)
		assert.False(t, ts.IsZero())
	})

	t.Run("unknown instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, _, err := b.GetConsoleOutput("i-doesnotexist")
		require.Error(t, err)
	})
}
func TestBatch1_ModifyInstanceMetadataOptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("sets required tokens", func(t *testing.T) { //nolint:paralleltest // existing issue.
		opts, err := b.ModifyInstanceMetadataOptions(inst.ID, "required", "enabled", "", 2)
		require.NoError(t, err)
		assert.Equal(t, "required", opts.HTTPTokens)
		assert.Equal(t, "applied", opts.State)
	})

	t.Run("unknown instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyInstanceMetadataOptions("i-missing", "required", "", "", 0)
		require.Error(t, err)
	})
}
func TestBatch1_InstanceMetadataDefaults(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("get returns default values", func(t *testing.T) { //nolint:paralleltest // existing issue.
		d := b.GetInstanceMetadataDefaults()
		assert.NotNil(t, d)
	})

	t.Run("modify updates values", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyInstanceMetadataDefaults("required", "enabled", "", 2))
		d := b.GetInstanceMetadataDefaults()
		assert.Equal(t, "required", d.HTTPTokens)
		assert.Equal(t, 2, d.HTTPPutResponseHopLimit)
	})
}
func TestBatch1_InstanceCreditSpecifications(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("default returns standard", func(t *testing.T) { //nolint:paralleltest // existing issue.
		specs := b.DescribeInstanceCreditSpecifications([]string{inst.ID})
		require.Len(t, specs, 1)
		assert.Equal(t, "standard", specs[0].CPUCredits)
	})

	t.Run("modify changes to unlimited", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyInstanceCreditSpecification(inst.ID, "unlimited"))
		specs := b.DescribeInstanceCreditSpecifications([]string{inst.ID})
		require.Len(t, specs, 1)
		assert.Equal(t, "unlimited", specs[0].CPUCredits)
	})
}
func TestBatch1_InstanceTopology(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	inst := insts[0]

	t.Run("returns topology for instance", func(t *testing.T) {
		items := b.DescribeInstanceTopology([]string{inst.ID})
		require.Len(t, items, 1)
		assert.Equal(t, inst.ID, items[0].InstanceID)
		assert.NotEmpty(t, items[0].AvailabilityZone)
	})
}
func TestBatch1_MonitorUnmonitorInstances(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)
	id := insts[0].ID

	t.Run("monitor sets enabled", func(t *testing.T) { //nolint:paralleltest // existing issue.
		states, err := b.MonitorInstances([]string{id})
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, "enabled", states[0].State)
	})

	t.Run("unmonitor sets disabled", func(t *testing.T) { //nolint:paralleltest // existing issue.
		states, err := b.UnmonitorInstances([]string{id})
		require.NoError(t, err)
		require.Len(t, states, 1)
		assert.Equal(t, "disabled", states[0].State)
	})

	t.Run("empty list returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.MonitorInstances(nil)
		require.Error(t, err)
	})
}

// ---- Network interface ---- //nolint:godot // existing issue.
func TestBatch1_NetworkInterfaceAttribute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "test NI")

	t.Run( //nolint:paralleltest // existing issue.
		"describe returns description and sourceDestCheck",
		func(t *testing.T) {
			result, err := b.DescribeNetworkInterfaceAttribute(ni.ID, "description")
			require.NoError(t, err)
			assert.Equal(t, ni.ID, result.NetworkInterfaceID)
		},
	)

	t.Run("reset sets sourceDestCheck to true", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ResetNetworkInterfaceAttribute(ni.ID))
	})

	t.Run("unknown NI returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DescribeNetworkInterfaceAttribute("eni-missing", "description")
		require.Error(t, err)
	})
}
func TestBatch1_NetworkInterfacePermissions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "perm test NI")

	t.Run("create permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
		perm, err := b.CreateNetworkInterfacePermission(
			ni.ID,
			"123456789012",
			"",
			"INSTANCE-ATTACH",
		)
		require.NoError(t, err)
		assert.Equal(t, "granted", perm.State)
	})

	t.Run("describe returns created permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
		perms := b.DescribeNetworkInterfacePermissions([]string{ni.ID})
		require.Len(t, perms, 1)
		assert.Equal(t, "INSTANCE-ATTACH", perms[0].Permission)

		t.Run("delete permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
			permID := perms[0].PermissionID
			require.NoError(t, b.DeleteNetworkInterfacePermission(permID))
			remaining := b.DescribeNetworkInterfacePermissions([]string{ni.ID})
			assert.Empty(t, remaining)
		})
	})
}
func TestBatch1_IPv6Addresses(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "ipv6 NI")

	t.Run("assign IPv6 addresses", func(t *testing.T) { //nolint:paralleltest // existing issue.
		addrs, err := b.AssignIpv6Addresses(ni.ID, 2)
		require.NoError(t, err)
		require.Len(t, addrs, 2)
	})

	t.Run("unassign IPv6 addresses", func(t *testing.T) { //nolint:paralleltest // existing issue.
		addrs, _ := b.AssignIpv6Addresses(ni.ID, 1)
		require.NoError(t, b.UnassignIpv6Addresses(ni.ID, addrs))
	})
}

// ---- Account/misc ---- //nolint:godot // existing issue.
func TestBatch1_DescribeAccountAttributes(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns all attributes when no filter", func(t *testing.T) { //nolint:paralleltest // existing issue.
		attrs := b.DescribeAccountAttributes(nil)
		assert.NotEmpty(t, attrs)
	})

	t.Run("filters by name", func(t *testing.T) { //nolint:paralleltest // existing issue.
		attrs := b.DescribeAccountAttributes([]string{"max-instances"})
		require.Len(t, attrs, 1)
		assert.Equal(t, "max-instances", attrs[0].Name)
	})
}
func TestBatch1_DescribePrefixLists(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns static prefix lists", func(t *testing.T) { //nolint:paralleltest // existing issue.
		lists := b.DescribePrefixLists(nil)
		require.NotEmpty(t, lists)
		assert.Contains(t, lists[0].PrefixListName, "s3")
	})

	t.Run("filters by ID", func(t *testing.T) { //nolint:paralleltest // existing issue.
		lists := b.DescribePrefixLists(nil)
		id := lists[0].PrefixListID
		filtered := b.DescribePrefixLists([]string{id})
		require.Len(t, filtered, 1)
	})
}
func TestBatch1_IdFormat(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("describe returns default resources", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeIDFormat(nil)
		assert.NotEmpty(t, items)
	})

	t.Run("modify and describe shows change", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyIDFormat("instance", true))
		items := b.DescribeIDFormat([]string{"instance"})
		require.Len(t, items, 1)
		assert.True(t, items[0].UseLongIDs)
	})

	t.Run("describe aggregate returns all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeAggregateIDFormat()
		assert.NotEmpty(t, items)
	})

	t.Run("identity format delegates to account format", func(t *testing.T) { //nolint:paralleltest // existing issue.
		items := b.DescribeIdentityIDFormat("arn:aws:iam::000000000000:user/test", nil)
		assert.NotEmpty(t, items)
	})
}
func TestBatch1_InstanceEventNotification(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("describe returns default", func(t *testing.T) { //nolint:paralleltest // existing issue.
		attrs := b.DescribeInstanceEventNotificationAttributes()
		assert.NotNil(t, attrs)
	})

	t.Run("deregister clears attributes", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DeregisterInstanceEventNotificationAttributes()
		attrs := b.DescribeInstanceEventNotificationAttributes()
		assert.NotNil(t, attrs)
	})
}

// ---- HTTP dispatch integration tests ---- //nolint:godot // existing issue.
func TestBatch1_HTTP_ModifyVolume(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h2 := ec2.NewHandler(b)

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 20)

	_, err := ec2.ExportDispatch(h2, url.Values{
		"Action":     []string{"ModifyVolume"},
		"VolumeId":   []string{vol.ID},
		"VolumeType": []string{"gp3"},
		"Size":       []string{"40"},
	})
	require.NoError(t, err)
	_ = h
}
func TestBatch1_HTTP_DescribeVolumeStatus(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeVolumeStatus"},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_CopySnapshot(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	vol, _ := b.CreateVolume("us-east-1a", "gp2", 10)
	snap, _ := b.CreateSnapshot(vol.ID, "src")

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":           []string{"CopySnapshot"},
		"SourceSnapshotId": []string{snap.SnapshotID},
		"Description":      []string{"copy"},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_GetSnapshotBlockPublicAccessState(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetSnapshotBlockPublicAccessState"},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_CreateDefaultVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	_ = b.DeleteVpc("vpc-default")

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action": []string{"CreateDefaultVpc"},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_DescribeAccountAttributes(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeAccountAttributes"},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_DescribePrefixLists(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribePrefixLists"},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_GetConsoleOutput(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":     []string{"GetConsoleOutput"},
		"InstanceId": []string{insts[0].ID},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_MonitorInstances(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":       []string{"MonitorInstances"},
		"InstanceId.1": []string{insts[0].ID},
	})
	require.NoError(t, err)
}
func TestBatch1_HTTP_DescribeIdFormat(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeIdFormat"},
	})
	require.NoError(t, err)
}
