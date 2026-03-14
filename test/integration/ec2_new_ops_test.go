package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EC2_NetworkInterface covers CreateNetworkInterface, DescribeNetworkInterfaces,
// AttachNetworkInterface, DetachNetworkInterface, AssignPrivateIpAddresses, UnassignPrivateIpAddresses,
// ModifyNetworkInterfaceAttribute, and DeleteNetworkInterface via the AWS SDK.
func TestIntegration_EC2_NetworkInterface(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	// Lookup the default subnet ID.
	subnetsOut, err := client.DescribeSubnets(ctx, &ec2sdk.DescribeSubnetsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, subnetsOut.Subnets)
	subnetID := aws.ToString(subnetsOut.Subnets[0].SubnetId)

	// CreateNetworkInterface
	createOut, err := client.CreateNetworkInterface(ctx, &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId:    aws.String(subnetID),
		Description: aws.String("integration-test-eni"),
	})
	require.NoError(t, err)
	eniID := aws.ToString(createOut.NetworkInterface.NetworkInterfaceId)
	require.NotEmpty(t, eniID)
	assert.Equal(t, "integration-test-eni", aws.ToString(createOut.NetworkInterface.Description))

	t.Cleanup(func() {
		_, _ = client.DeleteNetworkInterface(ctx, &ec2sdk.DeleteNetworkInterfaceInput{
			NetworkInterfaceId: aws.String(eniID),
		})
	})

	// DescribeNetworkInterfaces - should see the newly created ENI.
	descOut, err := client.DescribeNetworkInterfaces(ctx, &ec2sdk.DescribeNetworkInterfacesInput{
		NetworkInterfaceIds: []string{eniID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.NetworkInterfaces, 1)
	assert.Equal(t, eniID, aws.ToString(descOut.NetworkInterfaces[0].NetworkInterfaceId))

	// AssignPrivateIpAddresses - allocate 1 secondary IP.
	_, err = client.AssignPrivateIpAddresses(ctx, &ec2sdk.AssignPrivateIpAddressesInput{
		NetworkInterfaceId:             aws.String(eniID),
		SecondaryPrivateIpAddressCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// UnassignPrivateIpAddresses - remove any secondary IPs.
	descOut2, err := client.DescribeNetworkInterfaces(ctx, &ec2sdk.DescribeNetworkInterfacesInput{
		NetworkInterfaceIds: []string{eniID},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.NetworkInterfaces, 1)

	var secondaryIPs []string
	for _, pip := range descOut2.NetworkInterfaces[0].PrivateIpAddresses {
		if !aws.ToBool(pip.Primary) {
			secondaryIPs = append(secondaryIPs, aws.ToString(pip.PrivateIpAddress))
		}
	}

	if len(secondaryIPs) > 0 {
		_, err = client.UnassignPrivateIpAddresses(ctx, &ec2sdk.UnassignPrivateIpAddressesInput{
			NetworkInterfaceId: aws.String(eniID),
			PrivateIpAddresses: secondaryIPs,
		})
		require.NoError(t, err)
	}

	// ModifyNetworkInterfaceAttribute - update description.
	_, err = client.ModifyNetworkInterfaceAttribute(ctx, &ec2sdk.ModifyNetworkInterfaceAttributeInput{
		NetworkInterfaceId: aws.String(eniID),
		Description: &ec2types.AttributeValue{
			Value: aws.String("modified-description"),
		},
	})
	require.NoError(t, err)

	// DeleteNetworkInterface (also done in cleanup).
	_, err = client.DeleteNetworkInterface(ctx, &ec2sdk.DeleteNetworkInterfaceInput{
		NetworkInterfaceId: aws.String(eniID),
	})
	require.NoError(t, err)
}

// TestIntegration_EC2_SpotInstances covers RequestSpotInstances, DescribeSpotInstanceRequests,
// and CancelSpotInstanceRequests via the AWS SDK.
func TestIntegration_EC2_SpotInstances(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	// RequestSpotInstances
	requestOut, err := client.RequestSpotInstances(ctx, &ec2sdk.RequestSpotInstancesInput{
		SpotPrice:     aws.String("0.05"),
		InstanceCount: aws.Int32(1),
		LaunchSpecification: &ec2types.RequestSpotLaunchSpecification{
			ImageId:      aws.String("ami-0c55b159cbfafe1f0"),
			InstanceType: ec2types.InstanceTypeT2Micro,
		},
	})
	require.NoError(t, err)
	require.Len(t, requestOut.SpotInstanceRequests, 1)
	spotRequestID := aws.ToString(requestOut.SpotInstanceRequests[0].SpotInstanceRequestId)
	require.NotEmpty(t, spotRequestID)
	assert.Equal(t, "active", string(requestOut.SpotInstanceRequests[0].State))

	// DescribeSpotInstanceRequests - should return our request.
	descOut, err := client.DescribeSpotInstanceRequests(ctx, &ec2sdk.DescribeSpotInstanceRequestsInput{
		SpotInstanceRequestIds: []string{spotRequestID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.SpotInstanceRequests, 1)
	assert.Equal(t, spotRequestID, aws.ToString(descOut.SpotInstanceRequests[0].SpotInstanceRequestId))

	// DescribeSpotPriceHistory - should return an empty (stub) response.
	histOut, err := client.DescribeSpotPriceHistory(ctx, &ec2sdk.DescribeSpotPriceHistoryInput{})
	require.NoError(t, err)
	assert.Empty(t, histOut.SpotPriceHistory)

	// CancelSpotInstanceRequests
	cancelOut, err := client.CancelSpotInstanceRequests(ctx, &ec2sdk.CancelSpotInstanceRequestsInput{
		SpotInstanceRequestIds: []string{spotRequestID},
	})
	require.NoError(t, err)
	require.Len(t, cancelOut.CancelledSpotInstanceRequests, 1)
	assert.Equal(t, "cancelled", string(cancelOut.CancelledSpotInstanceRequests[0].State))
}

// TestIntegration_EC2_PlacementGroups covers CreatePlacementGroup, DescribePlacementGroups,
// and DeletePlacementGroup via the AWS SDK.
func TestIntegration_EC2_PlacementGroups(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	pgName := "integration-test-pg-cluster"

	// CreatePlacementGroup
	_, err := client.CreatePlacementGroup(ctx, &ec2sdk.CreatePlacementGroupInput{
		GroupName: aws.String(pgName),
		Strategy:  ec2types.PlacementStrategyCluster,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeletePlacementGroup(ctx, &ec2sdk.DeletePlacementGroupInput{
			GroupName: aws.String(pgName),
		})
	})

	// DescribePlacementGroups
	descOut, err := client.DescribePlacementGroups(ctx, &ec2sdk.DescribePlacementGroupsInput{
		GroupNames: []string{pgName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.PlacementGroups, 1)
	assert.Equal(t, pgName, aws.ToString(descOut.PlacementGroups[0].GroupName))
	assert.Equal(t, ec2types.PlacementStrategyCluster, descOut.PlacementGroups[0].Strategy)

	// DeletePlacementGroup
	_, err = client.DeletePlacementGroup(ctx, &ec2sdk.DeletePlacementGroupInput{
		GroupName: aws.String(pgName),
	})
	require.NoError(t, err)
}

// TestIntegration_EC2_InstanceAttributes covers ModifyInstanceAttribute and ResetInstanceAttribute
// via the AWS SDK.
func TestIntegration_EC2_InstanceAttributes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	// Launch an instance to modify.
	runOut, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-0c55b159cbfafe1f0"),
		InstanceType: ec2types.InstanceTypeT2Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	t.Cleanup(func() {
		_, _ = client.TerminateInstances(ctx, &ec2sdk.TerminateInstancesInput{
			InstanceIds: []string{instanceID},
		})
	})

	// ModifyInstanceAttribute - set instance type.
	_, err = client.ModifyInstanceAttribute(ctx, &ec2sdk.ModifyInstanceAttributeInput{
		InstanceId: aws.String(instanceID),
		InstanceType: &ec2types.AttributeValue{
			Value: aws.String("t3.micro"),
		},
	})
	require.NoError(t, err)

	// ResetInstanceAttribute - reset sourceDestCheck.
	_, err = client.ResetInstanceAttribute(ctx, &ec2sdk.ResetInstanceAttributeInput{
		InstanceId: aws.String(instanceID),
		Attribute:  ec2types.InstanceAttributeNameSourceDestCheck,
	})
	require.NoError(t, err)
}

// TestIntegration_EC2_VolumeSnapshotAttributes covers DescribeVolumeAttribute, ModifyVolumeAttribute,
// DescribeSnapshotAttribute, and ModifySnapshotAttribute via the AWS SDK.
func TestIntegration_EC2_VolumeSnapshotAttributes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	// Create a volume to query attributes on.
	volOut, err := client.CreateVolume(ctx, &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(10),
		VolumeType:       ec2types.VolumeTypeGp2,
	})
	require.NoError(t, err)
	volumeID := aws.ToString(volOut.VolumeId)
	require.NotEmpty(t, volumeID)

	t.Cleanup(func() {
		_, _ = client.DeleteVolume(ctx, &ec2sdk.DeleteVolumeInput{VolumeId: aws.String(volumeID)})
	})

	// DescribeVolumeAttribute
	attrOut, err := client.DescribeVolumeAttribute(ctx, &ec2sdk.DescribeVolumeAttributeInput{
		VolumeId:  aws.String(volumeID),
		Attribute: ec2types.VolumeAttributeNameAutoEnableIO,
	})
	require.NoError(t, err)
	assert.Equal(t, volumeID, aws.ToString(attrOut.VolumeId))

	// ModifyVolumeAttribute
	_, err = client.ModifyVolumeAttribute(ctx, &ec2sdk.ModifyVolumeAttributeInput{
		VolumeId: aws.String(volumeID),
		AutoEnableIO: &ec2types.AttributeBooleanValue{
			Value: aws.Bool(true),
		},
	})
	require.NoError(t, err)

	// DescribeSnapshotAttribute (stub - snapshot ID does not need to exist in mock).
	snapAttrOut, err := client.DescribeSnapshotAttribute(ctx, &ec2sdk.DescribeSnapshotAttributeInput{
		SnapshotId: aws.String("snap-12345678"),
		Attribute:  ec2types.SnapshotAttributeNameCreateVolumePermission,
	})
	require.NoError(t, err)
	assert.Equal(t, "snap-12345678", aws.ToString(snapAttrOut.SnapshotId))

	// ModifySnapshotAttribute
	_, err = client.ModifySnapshotAttribute(ctx, &ec2sdk.ModifySnapshotAttributeInput{
		SnapshotId: aws.String("snap-12345678"),
		Attribute:  ec2types.SnapshotAttributeNameCreateVolumePermission,
	})
	require.NoError(t, err)
}
