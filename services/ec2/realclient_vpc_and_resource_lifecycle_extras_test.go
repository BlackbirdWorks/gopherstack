package ec2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_VPCAndResourceLifecycleExtras covers ec2's remaining uncovered-op families
// (gopherstack-n3zi): VPC Block Public Access, VPC peering,
// volume/snapshot lifecycle extras, image lifecycle extras, security group
// extras, NAT gateway address management, network interface extras, subnet/
// IGW extras and transit gateway extras. Each row gets its own fresh
// handler+backend so rows can run in parallel without shared state.
func TestRealClient_VPCAndResourceLifecycleExtras(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *ec2sdk.Client)
		name string
	}{
		{runVpcBlockPublicAccess, "vpc_block_public_access"},
		{runVpcPeeringLifecycle, "vpc_peering_lifecycle"},
		{runVolumeLifecycleExtras, "volume_lifecycle_extras"},
		{runSnapshotLifecycleExtras, "snapshot_lifecycle_extras"},
		{runRecycleBinOps, "recycle_bin_ops"},
		{runImageLifecycleExtras, "image_lifecycle_extras"},
		{runAllowedImagesSettings, "allowed_images_settings"},
		{runSecurityGroupExtras, "security_group_extras"},
		{runNatGatewayAddressLifecycle, "nat_gateway_address_lifecycle"},
		{runNetworkInterfaceExtras, "network_interface_extras"},
		{runSubnetAndIgwExtras, "subnet_and_igw_extras"},
		{runTransitGatewayExtras, "transit_gateway_extras"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestEC2Client(t, h)
			tt.run(t, client)
		})
	}
}

// runVpcBlockPublicAccess covers ModifyVpcBlockPublicAccessOptions,
// DescribeVpcBlockPublicAccessOptions, CreateVpcBlockPublicAccessExclusion,
// ModifyVpcBlockPublicAccessExclusion, DescribeVpcBlockPublicAccessExclusions
// and DeleteVpcBlockPublicAccessExclusion.
func runVpcBlockPublicAccess(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	modOut, err := client.ModifyVpcBlockPublicAccessOptions(t.Context(), &ec2sdk.ModifyVpcBlockPublicAccessOptionsInput{
		InternetGatewayBlockMode: types.InternetGatewayBlockModeBlockIngress,
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.VpcBlockPublicAccessOptions)
	assert.Equal(
		t, types.InternetGatewayBlockModeBlockIngress,
		modOut.VpcBlockPublicAccessOptions.InternetGatewayBlockMode,
	)

	descOut, err := client.DescribeVpcBlockPublicAccessOptions(
		t.Context(), &ec2sdk.DescribeVpcBlockPublicAccessOptionsInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, descOut.VpcBlockPublicAccessOptions)
	assert.Equal(
		t, types.InternetGatewayBlockModeBlockIngress,
		descOut.VpcBlockPublicAccessOptions.InternetGatewayBlockMode,
	)

	exclOut, err := client.CreateVpcBlockPublicAccessExclusion(
		t.Context(), &ec2sdk.CreateVpcBlockPublicAccessExclusionInput{
			VpcId:                        aws.String(vpcID),
			InternetGatewayExclusionMode: types.InternetGatewayExclusionModeAllowEgress,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, exclOut.VpcBlockPublicAccessExclusion)
	exclusionID := aws.ToString(exclOut.VpcBlockPublicAccessExclusion.ExclusionId)
	assert.NotEmpty(t, exclusionID)
	assert.Equal(
		t, types.InternetGatewayExclusionModeAllowEgress,
		exclOut.VpcBlockPublicAccessExclusion.InternetGatewayExclusionMode,
	)

	modExclOut, err := client.ModifyVpcBlockPublicAccessExclusion(
		t.Context(), &ec2sdk.ModifyVpcBlockPublicAccessExclusionInput{
			ExclusionId:                  aws.String(exclusionID),
			InternetGatewayExclusionMode: types.InternetGatewayExclusionModeAllowBidirectional,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t, types.InternetGatewayExclusionModeAllowBidirectional,
		modExclOut.VpcBlockPublicAccessExclusion.InternetGatewayExclusionMode,
	)

	listOut, err := client.DescribeVpcBlockPublicAccessExclusions(
		t.Context(), &ec2sdk.DescribeVpcBlockPublicAccessExclusionsInput{
			ExclusionIds: []string{exclusionID},
		},
	)
	require.NoError(t, err)
	require.Len(t, listOut.VpcBlockPublicAccessExclusions, 1)
	assert.Equal(t, exclusionID, aws.ToString(listOut.VpcBlockPublicAccessExclusions[0].ExclusionId))

	delOut, err := client.DeleteVpcBlockPublicAccessExclusion(
		t.Context(), &ec2sdk.DeleteVpcBlockPublicAccessExclusionInput{ExclusionId: aws.String(exclusionID)},
	)
	require.NoError(t, err)
	require.NotNil(t, delOut.VpcBlockPublicAccessExclusion)
}

// runVpcPeeringLifecycle covers AcceptVpcPeeringConnection,
// ModifyVpcPeeringConnectionOptions, RejectVpcPeeringConnection and
// DeleteVpcPeeringConnection.
func runVpcPeeringLifecycle(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	requesterOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.1.0.0/16")})
	require.NoError(t, err)
	requesterID := aws.ToString(requesterOut.Vpc.VpcId)

	accepterOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.2.0.0/16")})
	require.NoError(t, err)
	accepterID := aws.ToString(accepterOut.Vpc.VpcId)

	pcxOut, err := client.CreateVpcPeeringConnection(t.Context(), &ec2sdk.CreateVpcPeeringConnectionInput{
		VpcId: aws.String(requesterID), PeerVpcId: aws.String(accepterID),
	})
	require.NoError(t, err)
	require.NotNil(t, pcxOut.VpcPeeringConnection)
	pcxID := aws.ToString(pcxOut.VpcPeeringConnection.VpcPeeringConnectionId)
	require.NotEmpty(t, pcxID)

	acceptOut, err := client.AcceptVpcPeeringConnection(t.Context(), &ec2sdk.AcceptVpcPeeringConnectionInput{
		VpcPeeringConnectionId: aws.String(pcxID),
	})
	require.NoError(t, err)
	require.NotNil(t, acceptOut.VpcPeeringConnection)
	assert.Equal(t, pcxID, aws.ToString(acceptOut.VpcPeeringConnection.VpcPeeringConnectionId))

	modOut, err := client.ModifyVpcPeeringConnectionOptions(
		t.Context(), &ec2sdk.ModifyVpcPeeringConnectionOptionsInput{
			VpcPeeringConnectionId: aws.String(pcxID),
			RequesterPeeringConnectionOptions: &types.PeeringConnectionOptionsRequest{
				AllowDnsResolutionFromRemoteVpc: aws.Bool(true),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modOut.RequesterPeeringConnectionOptions)
	assert.True(t, aws.ToBool(modOut.RequesterPeeringConnectionOptions.AllowDnsResolutionFromRemoteVpc))

	secondPcxOut, err := client.CreateVpcPeeringConnection(t.Context(), &ec2sdk.CreateVpcPeeringConnectionInput{
		VpcId: aws.String(requesterID), PeerVpcId: aws.String(accepterID),
	})
	require.NoError(t, err)
	secondPcxID := aws.ToString(secondPcxOut.VpcPeeringConnection.VpcPeeringConnectionId)

	rejectOut, err := client.RejectVpcPeeringConnection(t.Context(), &ec2sdk.RejectVpcPeeringConnectionInput{
		VpcPeeringConnectionId: aws.String(secondPcxID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(rejectOut.Return))

	delOut, err := client.DeleteVpcPeeringConnection(t.Context(), &ec2sdk.DeleteVpcPeeringConnectionInput{
		VpcPeeringConnectionId: aws.String(pcxID),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(delOut.Return))
}

// runVolumeLifecycleExtras covers ModifyVolume, EnableVolumeIO,
// DescribeVolumeStatus, DescribeVolumesModifications,
// CreateReplaceRootVolumeTask and DescribeReplaceRootVolumeTasks.
func runVolumeLifecycleExtras(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)
	az := aws.ToString(runOut.Instances[0].Placement.AvailabilityZone)

	volOut, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String(az), Size: aws.Int32(8), VolumeType: types.VolumeTypeGp3,
	})
	require.NoError(t, err)
	volumeID := aws.ToString(volOut.VolumeId)

	modOut, err := client.ModifyVolume(t.Context(), &ec2sdk.ModifyVolumeInput{
		VolumeId: aws.String(volumeID), Size: aws.Int32(16),
	})
	require.NoError(t, err)
	require.NotNil(t, modOut.VolumeModification)
	assert.Equal(t, volumeID, aws.ToString(modOut.VolumeModification.VolumeId))
	assert.Equal(t, int32(16), aws.ToInt32(modOut.VolumeModification.TargetSize))

	_, err = client.EnableVolumeIO(t.Context(), &ec2sdk.EnableVolumeIOInput{VolumeId: aws.String(volumeID)})
	require.NoError(t, err)

	statusOut, err := client.DescribeVolumeStatus(t.Context(), &ec2sdk.DescribeVolumeStatusInput{
		VolumeIds: []string{volumeID},
	})
	require.NoError(t, err)
	require.Len(t, statusOut.VolumeStatuses, 1)
	assert.Equal(t, volumeID, aws.ToString(statusOut.VolumeStatuses[0].VolumeId))

	modsOut, err := client.DescribeVolumesModifications(t.Context(), &ec2sdk.DescribeVolumesModificationsInput{
		VolumeIds: []string{volumeID},
	})
	require.NoError(t, err)
	require.Len(t, modsOut.VolumesModifications, 1)
	assert.Equal(t, volumeID, aws.ToString(modsOut.VolumesModifications[0].VolumeId))

	rrvOut, err := client.CreateReplaceRootVolumeTask(t.Context(), &ec2sdk.CreateReplaceRootVolumeTaskInput{
		InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)
	require.NotNil(t, rrvOut.ReplaceRootVolumeTask)
	taskID := aws.ToString(rrvOut.ReplaceRootVolumeTask.ReplaceRootVolumeTaskId)
	assert.NotEmpty(t, taskID)

	tasksOut, err := client.DescribeReplaceRootVolumeTasks(t.Context(), &ec2sdk.DescribeReplaceRootVolumeTasksInput{
		ReplaceRootVolumeTaskIds: []string{taskID},
	})
	require.NoError(t, err)
	require.Len(t, tasksOut.ReplaceRootVolumeTasks, 1)
	assert.Equal(t, instanceID, aws.ToString(tasksOut.ReplaceRootVolumeTasks[0].InstanceId))
}

// runSnapshotLifecycleExtras covers ModifySnapshotTier, RestoreSnapshotTier,
// GetSnapshotBlockPublicAccessState, EnableSnapshotBlockPublicAccess,
// DisableSnapshotBlockPublicAccess, ResetSnapshotAttribute and
// DescribeFastSnapshotRestores.
func runSnapshotLifecycleExtras(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	volOut, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8),
	})
	require.NoError(t, err)
	volumeID := aws.ToString(volOut.VolumeId)

	snapOut, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: aws.String(volumeID)})
	require.NoError(t, err)
	snapshotID := aws.ToString(snapOut.SnapshotId)

	tierOut, err := client.ModifySnapshotTier(t.Context(), &ec2sdk.ModifySnapshotTierInput{
		SnapshotId: aws.String(snapshotID), StorageTier: types.TargetStorageTierArchive,
	})
	require.NoError(t, err)
	assert.Equal(t, snapshotID, aws.ToString(tierOut.SnapshotId))

	restoreOut, err := client.RestoreSnapshotTier(t.Context(), &ec2sdk.RestoreSnapshotTierInput{
		SnapshotId: aws.String(snapshotID),
	})
	require.NoError(t, err)
	assert.Equal(t, snapshotID, aws.ToString(restoreOut.SnapshotId))

	stateOut, err := client.GetSnapshotBlockPublicAccessState(
		t.Context(), &ec2sdk.GetSnapshotBlockPublicAccessStateInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, stateOut.State)

	enableOut, err := client.EnableSnapshotBlockPublicAccess(
		t.Context(), &ec2sdk.EnableSnapshotBlockPublicAccessInput{
			State: types.SnapshotBlockPublicAccessStateBlockAllSharing,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.SnapshotBlockPublicAccessStateBlockAllSharing, enableOut.State)

	disableOut, err := client.DisableSnapshotBlockPublicAccess(
		t.Context(), &ec2sdk.DisableSnapshotBlockPublicAccessInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, disableOut.State)

	_, err = client.ResetSnapshotAttribute(t.Context(), &ec2sdk.ResetSnapshotAttributeInput{
		SnapshotId: aws.String(snapshotID), Attribute: types.SnapshotAttributeNameCreateVolumePermission,
	})
	require.NoError(t, err)

	restoresOut, err := client.DescribeFastSnapshotRestores(
		t.Context(), &ec2sdk.DescribeFastSnapshotRestoresInput{},
	)
	require.NoError(t, err)
	assert.NotNil(t, restoresOut.FastSnapshotRestores)
}

// runRecycleBinOps covers ListSnapshotsInRecycleBin,
// RestoreSnapshotFromRecycleBin, ListVolumesInRecycleBin and
// RestoreVolumeFromRecycleBin. Nothing in this backend ever populates either
// recycle bin (services/ec2/handler_snapshots.go documents the snapshot side
// of this gap; PARITY.md items_still_open), so these assert the real,
// achievable behavior: an empty list, and NotFound on an id that was never
// recycled -- not a fabricated "recycled" round trip.
func runRecycleBinOps(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	snapList, err := client.ListSnapshotsInRecycleBin(t.Context(), &ec2sdk.ListSnapshotsInRecycleBinInput{})
	require.NoError(t, err)
	assert.Empty(t, snapList.Snapshots)

	_, err = client.RestoreSnapshotFromRecycleBin(t.Context(), &ec2sdk.RestoreSnapshotFromRecycleBinInput{
		SnapshotId: aws.String("snap-not-in-recycle-bin"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidSnapshotID.NotFound")

	volList, err := client.ListVolumesInRecycleBin(t.Context(), &ec2sdk.ListVolumesInRecycleBinInput{})
	require.NoError(t, err)
	assert.Empty(t, volList.Volumes)

	_, err = client.RestoreVolumeFromRecycleBin(t.Context(), &ec2sdk.RestoreVolumeFromRecycleBinInput{
		VolumeId: aws.String("vol-not-in-recycle-bin"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidVolume.NotFound")
}

// runImageLifecycleExtras covers CreateImage, EnableImage,
// EnableImageDeprecation, DisableImageDeprecation,
// EnableImageDeregistrationProtection, DisableImageDeregistrationProtection
// and ResetImageAttribute.
func runImageLifecycleExtras(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	imgOut, err := client.CreateImage(t.Context(), &ec2sdk.CreateImageInput{
		InstanceId: aws.String(instanceID), Name: aws.String("slice2-test-image"),
	})
	require.NoError(t, err)
	imageID := aws.ToString(imgOut.ImageId)
	require.NotEmpty(t, imageID)

	enableOut, err := client.EnableImage(t.Context(), &ec2sdk.EnableImageInput{ImageId: aws.String(imageID)})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(enableOut.Return))

	_, err = client.EnableImageDeprecation(t.Context(), &ec2sdk.EnableImageDeprecationInput{
		ImageId: aws.String(imageID), DeprecateAt: aws.Time(time.Now().Add(24 * time.Hour)),
	})
	require.NoError(t, err)

	_, err = client.DisableImageDeprecation(t.Context(), &ec2sdk.DisableImageDeprecationInput{
		ImageId: aws.String(imageID),
	})
	require.NoError(t, err)

	_, err = client.EnableImageDeregistrationProtection(
		t.Context(), &ec2sdk.EnableImageDeregistrationProtectionInput{ImageId: aws.String(imageID)},
	)
	require.NoError(t, err)

	_, err = client.DisableImageDeregistrationProtection(
		t.Context(), &ec2sdk.DisableImageDeregistrationProtectionInput{ImageId: aws.String(imageID)},
	)
	require.NoError(t, err)

	_, err = client.ResetImageAttribute(t.Context(), &ec2sdk.ResetImageAttributeInput{
		ImageId: aws.String(imageID), Attribute: types.ResetImageAttributeNameLaunchPermission,
	})
	require.NoError(t, err)
}

// runAllowedImagesSettings covers EnableAllowedImagesSettings,
// GetAllowedImagesSettings, ReplaceImageCriteriaInAllowedImagesSettings and
// DisableAllowedImagesSettings.
func runAllowedImagesSettings(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	enableOut, err := client.EnableAllowedImagesSettings(t.Context(), &ec2sdk.EnableAllowedImagesSettingsInput{
		AllowedImagesSettingsState: types.AllowedImagesSettingsEnabledStateEnabled,
	})
	require.NoError(t, err)
	assert.Equal(t, types.AllowedImagesSettingsEnabledStateEnabled, enableOut.AllowedImagesSettingsState)

	replaceOut, err := client.ReplaceImageCriteriaInAllowedImagesSettings(
		t.Context(), &ec2sdk.ReplaceImageCriteriaInAllowedImagesSettingsInput{
			ImageCriteria: []types.ImageCriterionRequest{{ImageNames: []string{"my-approved-ami-*"}}},
		},
	)
	require.NoError(t, err)
	assert.True(t, aws.ToBool(replaceOut.ReturnValue))

	getOut, err := client.GetAllowedImagesSettings(t.Context(), &ec2sdk.GetAllowedImagesSettingsInput{})
	require.NoError(t, err)
	assert.Equal(t, string(types.AllowedImagesSettingsEnabledStateEnabled), aws.ToString(getOut.State))
	require.Len(t, getOut.ImageCriteria, 1)
	assert.Equal(t, []string{"my-approved-ami-*"}, getOut.ImageCriteria[0].ImageNames)

	disableOut, err := client.DisableAllowedImagesSettings(
		t.Context(), &ec2sdk.DisableAllowedImagesSettingsInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AllowedImagesSettingsDisabledStateDisabled, disableOut.AllowedImagesSettingsState)
}

// runSecurityGroupExtras covers DescribeSecurityGroupReferences,
// DescribeStaleSecurityGroups and ModifySecurityGroupRules.
func runSecurityGroupExtras(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	sgOut, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName: aws.String("slice2-sg"), Description: aws.String("slice2 test sg"), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)
	groupID := aws.ToString(sgOut.GroupId)

	authOut, err := client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(groupID),
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"), FromPort: aws.Int32(22), ToPort: aws.Int32(22),
			IpRanges: []types.IpRange{{CidrIp: aws.String("10.0.0.0/24")}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, authOut.SecurityGroupRules, 1)
	ruleID := aws.ToString(authOut.SecurityGroupRules[0].SecurityGroupRuleId)
	require.NotEmpty(t, ruleID)

	refsOut, err := client.DescribeSecurityGroupReferences(t.Context(), &ec2sdk.DescribeSecurityGroupReferencesInput{
		GroupId: []string{groupID},
	})
	require.NoError(t, err)
	assert.NotNil(t, refsOut.SecurityGroupReferenceSet)

	staleOut, err := client.DescribeStaleSecurityGroups(t.Context(), &ec2sdk.DescribeStaleSecurityGroupsInput{
		VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)
	assert.Empty(t, staleOut.StaleSecurityGroupSet, "no deleted peer VPC exists, so nothing is stale")

	modOut, err := client.ModifySecurityGroupRules(t.Context(), &ec2sdk.ModifySecurityGroupRulesInput{
		GroupId: aws.String(groupID),
		SecurityGroupRules: []types.SecurityGroupRuleUpdate{{
			SecurityGroupRuleId: aws.String(ruleID),
			SecurityGroupRule: &types.SecurityGroupRuleRequest{
				IpProtocol: aws.String("tcp"), FromPort: aws.Int32(2222), ToPort: aws.Int32(2222),
				CidrIpv4: aws.String("10.0.0.0/24"), Description: aws.String("modified by slice2"),
			},
		}},
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modOut.Return))
}

// runNatGatewayAddressLifecycle covers AssociateNatGatewayAddress,
// DisassociateNatGatewayAddress, UnassignPrivateNatGatewayAddress,
// DescribeNatGateways and DeleteNatGateway.
func runNatGatewayAddressLifecycle(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	primaryAddr, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	ngwOut, err := client.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		SubnetId: aws.String(subnetID), AllocationId: primaryAddr.AllocationId,
	})
	require.NoError(t, err)
	natGatewayID := aws.ToString(ngwOut.NatGateway.NatGatewayId)

	secondaryAddr, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	// CreateNatGateway already put the primary allocation's address in
	// NatGatewayAddresses (toNatGatewayItem always includes it, IsPrimary
	// true), so associating one more allocation brings the total to 2.
	assocOut, err := client.AssociateNatGatewayAddress(t.Context(), &ec2sdk.AssociateNatGatewayAddressInput{
		NatGatewayId: aws.String(natGatewayID), AllocationIds: []string{aws.ToString(secondaryAddr.AllocationId)},
	})
	require.NoError(t, err)
	require.Len(t, assocOut.NatGatewayAddresses, 2)

	var assocID string
	for _, a := range assocOut.NatGatewayAddresses {
		if !aws.ToBool(a.IsPrimary) {
			assocID = aws.ToString(a.AssociationId)
		}
	}
	require.NotEmpty(t, assocID)

	describedOut, err := client.DescribeNatGateways(t.Context(), &ec2sdk.DescribeNatGatewaysInput{
		NatGatewayIds: []string{natGatewayID},
	})
	require.NoError(t, err)
	require.Len(t, describedOut.NatGateways, 1)
	assert.Len(t, describedOut.NatGateways[0].NatGatewayAddresses, 2)

	disassocOut, err := client.DisassociateNatGatewayAddress(t.Context(), &ec2sdk.DisassociateNatGatewayAddressInput{
		NatGatewayId: aws.String(natGatewayID), AssociationIds: []string{assocID},
	})
	require.NoError(t, err)
	assert.Len(t, disassocOut.NatGatewayAddresses, 1, "only the primary address remains after disassociation")

	// UnassignPrivateNatGatewayAddress no-ops on a private IP that was never
	// assigned to this NAT gateway rather than erroring (mirrors the backend's
	// own filter-by-membership implementation, nat_gateways.go).
	_, err = client.UnassignPrivateNatGatewayAddress(t.Context(), &ec2sdk.UnassignPrivateNatGatewayAddressInput{
		NatGatewayId: aws.String(natGatewayID), PrivateIpAddresses: []string{"10.0.1.99"},
	})
	require.NoError(t, err)

	_, err = client.DeleteNatGateway(t.Context(), &ec2sdk.DeleteNatGatewayInput{
		NatGatewayId: aws.String(natGatewayID),
	})
	require.NoError(t, err)

	_, err = client.DescribeNatGateways(t.Context(), &ec2sdk.DescribeNatGatewaysInput{
		NatGatewayIds: []string{natGatewayID},
	})
	require.Error(t, err, "deleted NAT gateway must NotFound when named explicitly, not silently vanish")
}

// runNetworkInterfaceExtras covers DetachNetworkInterface,
// CreateNetworkInterfacePermission, DeleteNetworkInterfacePermission and
// ResetNetworkInterfaceAttribute.
func runNetworkInterfaceExtras(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	eniOut, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: aws.String(subnetID),
	})
	require.NoError(t, err)
	eniID := aws.ToString(eniOut.NetworkInterface.NetworkInterfaceId)

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	attachOut, err := client.AttachNetworkInterface(t.Context(), &ec2sdk.AttachNetworkInterfaceInput{
		NetworkInterfaceId: aws.String(eniID), InstanceId: aws.String(instanceID), DeviceIndex: aws.Int32(1),
	})
	require.NoError(t, err)
	attachmentID := aws.ToString(attachOut.AttachmentId)
	require.NotEmpty(t, attachmentID)

	_, err = client.DetachNetworkInterface(t.Context(), &ec2sdk.DetachNetworkInterfaceInput{
		AttachmentId: aws.String(attachmentID),
	})
	require.NoError(t, err)

	permOut, err := client.CreateNetworkInterfacePermission(
		t.Context(), &ec2sdk.CreateNetworkInterfacePermissionInput{
			NetworkInterfaceId: aws.String(eniID),
			Permission:         types.InterfacePermissionTypeInstanceAttach,
			AwsAccountId:       aws.String("111111111111"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, permOut.InterfacePermission)
	permissionID := aws.ToString(permOut.InterfacePermission.NetworkInterfacePermissionId)
	require.NotEmpty(t, permissionID)

	_, err = client.DeleteNetworkInterfacePermission(t.Context(), &ec2sdk.DeleteNetworkInterfacePermissionInput{
		NetworkInterfacePermissionId: aws.String(permissionID),
	})
	require.NoError(t, err)

	_, err = client.ResetNetworkInterfaceAttribute(t.Context(), &ec2sdk.ResetNetworkInterfaceAttributeInput{
		NetworkInterfaceId: aws.String(eniID),
	})
	require.NoError(t, err)
}

// runSubnetAndIgwExtras covers ModifySubnetAttribute,
// DeleteSubnetCidrReservation, DeleteEgressOnlyInternetGateway and
// ReplaceRouteTableAssociation.
func runSubnetAndIgwExtras(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	_, err = client.ModifySubnetAttribute(t.Context(), &ec2sdk.ModifySubnetAttributeInput{
		SubnetId: aws.String(subnetID), MapPublicIpOnLaunch: &types.AttributeBooleanValue{Value: aws.Bool(true)},
	})
	require.NoError(t, err)

	describedSubnet, err := client.DescribeSubnets(t.Context(), &ec2sdk.DescribeSubnetsInput{
		SubnetIds: []string{subnetID},
	})
	require.NoError(t, err)
	require.Len(t, describedSubnet.Subnets, 1)
	assert.True(t, aws.ToBool(describedSubnet.Subnets[0].MapPublicIpOnLaunch))

	reservationOut, err := client.CreateSubnetCidrReservation(t.Context(), &ec2sdk.CreateSubnetCidrReservationInput{
		SubnetId: aws.String(subnetID), Cidr: aws.String("10.0.1.128/28"),
		ReservationType: types.SubnetCidrReservationTypePrefix,
	})
	require.NoError(t, err)
	reservationID := aws.ToString(reservationOut.SubnetCidrReservation.SubnetCidrReservationId)
	require.NotEmpty(t, reservationID)

	_, err = client.DeleteSubnetCidrReservation(t.Context(), &ec2sdk.DeleteSubnetCidrReservationInput{
		SubnetCidrReservationId: aws.String(reservationID),
	})
	require.NoError(t, err)

	eigwOut, err := client.CreateEgressOnlyInternetGateway(
		t.Context(), &ec2sdk.CreateEgressOnlyInternetGatewayInput{VpcId: aws.String(vpcID)},
	)
	require.NoError(t, err)
	eigwID := aws.ToString(eigwOut.EgressOnlyInternetGateway.EgressOnlyInternetGatewayId)
	require.NotEmpty(t, eigwID)

	_, err = client.DeleteEgressOnlyInternetGateway(
		t.Context(), &ec2sdk.DeleteEgressOnlyInternetGatewayInput{EgressOnlyInternetGatewayId: aws.String(eigwID)},
	)
	require.NoError(t, err)

	rtOut, err := client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: aws.String(vpcID)})
	require.NoError(t, err)
	rtID := aws.ToString(rtOut.RouteTable.RouteTableId)

	assocOut, err := client.AssociateRouteTable(t.Context(), &ec2sdk.AssociateRouteTableInput{
		RouteTableId: aws.String(rtID), SubnetId: aws.String(subnetID),
	})
	require.NoError(t, err)
	assocID := aws.ToString(assocOut.AssociationId)

	secondRtOut, err := client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: aws.String(vpcID)})
	require.NoError(t, err)
	secondRtID := aws.ToString(secondRtOut.RouteTable.RouteTableId)

	replaceOut, err := client.ReplaceRouteTableAssociation(t.Context(), &ec2sdk.ReplaceRouteTableAssociationInput{
		AssociationId: aws.String(assocID), RouteTableId: aws.String(secondRtID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(replaceOut.NewAssociationId))
}

// runTransitGatewayExtras covers DescribeTransitGateways, ModifyTransitGateway,
// ModifyTransitGatewayVpcAttachment and DeleteTransitGatewayVpcAttachment.
func runTransitGatewayExtras(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	tgwOut, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{
		Description: aws.String("slice2 tgw"),
	})
	require.NoError(t, err)
	tgwID := aws.ToString(tgwOut.TransitGateway.TransitGatewayId)

	describedOut, err := client.DescribeTransitGateways(t.Context(), &ec2sdk.DescribeTransitGatewaysInput{
		TransitGatewayIds: []string{tgwID},
	})
	require.NoError(t, err)
	require.Len(t, describedOut.TransitGateways, 1)

	modOut, err := client.ModifyTransitGateway(t.Context(), &ec2sdk.ModifyTransitGatewayInput{
		TransitGatewayId: aws.String(tgwID), Description: aws.String("modified by slice2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "modified by slice2", aws.ToString(modOut.TransitGateway.Description))

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.1.0/24"), AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)
	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	secondSubnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.0.2.0/24"), AvailabilityZone: aws.String("us-east-1b"),
	})
	require.NoError(t, err)
	secondSubnetID := aws.ToString(secondSubnetOut.Subnet.SubnetId)

	attOut, err := client.CreateTransitGatewayVpcAttachment(t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
		TransitGatewayId: aws.String(tgwID), VpcId: aws.String(vpcID), SubnetIds: []string{subnetID},
	})
	require.NoError(t, err)
	attachmentID := aws.ToString(attOut.TransitGatewayVpcAttachment.TransitGatewayAttachmentId)

	modAttOut, err := client.ModifyTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.ModifyTransitGatewayVpcAttachmentInput{
			TransitGatewayAttachmentId: aws.String(attachmentID), AddSubnetIds: []string{secondSubnetID},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, modAttOut.TransitGatewayVpcAttachment)
	assert.Contains(t, modAttOut.TransitGatewayVpcAttachment.SubnetIds, secondSubnetID)

	_, err = client.DeleteTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.DeleteTransitGatewayVpcAttachmentInput{
			TransitGatewayAttachmentId: aws.String(attachmentID),
		},
	)
	require.NoError(t, err)
}
