package terraform_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch11 provisions a broad set of previously-uncovered
// EC2 compute/storage resources (key pair, placement group, launch template,
// AMI family, EBS snapshot/encryption family, capacity reservation, a spot
// instance request, an EC2 fleet, dedicated host, managed prefix list, flow
// log, and single-VPC networking singletons) via Terraform and verifies each
// through the EC2 SDK.
func TestTerraform_MegaBatch11(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-11",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return vpcCIDRVars(t)
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()
				client := createEC2Client(t)

				vpcID := verifyMegaBatch11Networking(ctx, t, client, vars["VPCCidr"].(string))
				instanceID := verifyMegaBatch11Compute(ctx, t, client)
				verifyMegaBatch11Storage(ctx, t, client, instanceID)
				verifyMegaBatch11AccountSettings(ctx, t, client)
				verifyMegaBatch11InstanceState(ctx, t, client, instanceID)
				verifyMegaBatch11PrefixListAndTag(ctx, t, client, vpcID)
				verifyMegaBatch11FlowLogAndENI(ctx, t, client, vpcID)
				verifyMegaBatch11SpotAndFleet(ctx, t, client)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

// verifyMegaBatch11Networking checks the internet gateway attachment,
// egress-only internet gateway, main route table association, and network
// ACL association, returning the fixture's VPC ID for later use.
func verifyMegaBatch11Networking(ctx context.Context, t *testing.T, client *ec2svc.Client, vpcCidr string) string {
	t.Helper()

	vpcsOut, err := client.DescribeVpcs(ctx, &ec2svc.DescribeVpcsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("cidr-block"), Values: []string{vpcCidr}},
		},
	})
	require.NoError(t, err, "DescribeVpcs should succeed")
	require.Len(t, vpcsOut.Vpcs, 1)
	vpcID := aws.ToString(vpcsOut.Vpcs[0].VpcId)

	igwOut, err := client.DescribeInternetGateways(ctx, &ec2svc.DescribeInternetGatewaysInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("attachment.vpc-id"), Values: []string{vpcID}},
		},
	})
	require.NoError(t, err, "DescribeInternetGateways should succeed")
	require.Len(
		t,
		igwOut.InternetGateways,
		1,
		"internet gateway should be attached via aws_internet_gateway_attachment",
	)

	eoigwOut, err := client.DescribeEgressOnlyInternetGateways(ctx, &ec2svc.DescribeEgressOnlyInternetGatewaysInput{})
	require.NoError(t, err, "DescribeEgressOnlyInternetGateways should succeed")

	var foundEOIGW bool

	for _, gw := range eoigwOut.EgressOnlyInternetGateways {
		for _, att := range gw.Attachments {
			if aws.ToString(att.VpcId) == vpcID {
				foundEOIGW = true
			}
		}
	}

	assert.True(t, foundEOIGW, "egress-only internet gateway should be attached to the VPC")

	rtOut, err := client.DescribeRouteTables(ctx, &ec2svc.DescribeRouteTablesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{vpcID}},
			{Name: aws.String("association.main"), Values: []string{"true"}},
		},
	})
	require.NoError(t, err, "DescribeRouteTables should succeed")
	require.Len(
		t,
		rtOut.RouteTables,
		1,
		"aws_main_route_table_association should make the new route table the main one",
	)

	naclOut, err := client.DescribeNetworkAcls(ctx, &ec2svc.DescribeNetworkAclsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{vpcID}},
			{Name: aws.String("default"), Values: []string{"false"}},
		},
	})
	require.NoError(t, err, "DescribeNetworkAcls should succeed")
	require.NotEmpty(t, naclOut.NetworkAcls, "non-default network ACL should exist")

	var foundNACLAssoc bool

	for _, assoc := range naclOut.NetworkAcls[0].Associations {
		if aws.ToString(assoc.SubnetId) != "" {
			foundNACLAssoc = true
		}
	}

	assert.True(t, foundNACLAssoc, "aws_network_acl_association should associate the subnet")

	return vpcID
}

// verifyMegaBatch11Compute checks the key pair, placement group, dedicated
// host, capacity reservation, launch template, and instance, returning the
// created instance's ID for later use.
func verifyMegaBatch11Compute(ctx context.Context, t *testing.T, client *ec2svc.Client) string {
	t.Helper()

	kpOut, err := client.DescribeKeyPairs(ctx, &ec2svc.DescribeKeyPairsInput{
		KeyNames: []string{"mega-batch-11-key"},
	})
	require.NoError(t, err, "DescribeKeyPairs should succeed")
	require.Len(t, kpOut.KeyPairs, 1)

	pgOut, err := client.DescribePlacementGroups(ctx, &ec2svc.DescribePlacementGroupsInput{
		GroupNames: []string{"mega-batch-11-pg"},
	})
	require.NoError(t, err, "DescribePlacementGroups should succeed")
	require.Len(t, pgOut.PlacementGroups, 1)
	assert.Equal(t, ec2types.PlacementStrategyCluster, pgOut.PlacementGroups[0].Strategy)

	hostsOut, err := client.DescribeHosts(ctx, &ec2svc.DescribeHostsInput{})
	require.NoError(t, err, "DescribeHosts should succeed")

	var foundHost bool

	for _, h := range hostsOut.Hosts {
		if h.HostProperties != nil && aws.ToString(h.HostProperties.InstanceType) == "c5.large" {
			foundHost = true
		}
	}

	assert.True(t, foundHost, "dedicated host should be listed")

	crOut, err := client.DescribeCapacityReservations(ctx, &ec2svc.DescribeCapacityReservationsInput{})
	require.NoError(t, err, "DescribeCapacityReservations should succeed")

	var foundReservation bool

	for _, cr := range crOut.CapacityReservations {
		if aws.ToString(cr.InstanceType) == "t2.micro" && cr.AvailabilityZone != nil &&
			aws.ToString(cr.AvailabilityZone) == "us-east-1a" {
			foundReservation = true
		}
	}

	assert.True(t, foundReservation, "capacity reservation should be listed")

	ltOut, err := client.DescribeLaunchTemplates(ctx, &ec2svc.DescribeLaunchTemplatesInput{
		LaunchTemplateNames: []string{"mega-batch-11-lt"},
	})
	require.NoError(t, err, "DescribeLaunchTemplates should succeed")
	require.Len(t, ltOut.LaunchTemplates, 1)

	instOut, err := client.DescribeInstances(ctx, &ec2svc.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-11-instance"}},
		},
	})
	require.NoError(t, err, "DescribeInstances should succeed")
	require.Len(t, instOut.Reservations, 1)
	require.Len(t, instOut.Reservations[0].Instances, 1)

	return aws.ToString(instOut.Reservations[0].Instances[0].InstanceId)
}

// verifyMegaBatch11Storage checks the volume attachment, snapshot, snapshot
// create-volume permission, AMI, AMI copy, and AMI launch permission.
func verifyMegaBatch11Storage(ctx context.Context, t *testing.T, client *ec2svc.Client, instanceID string) {
	t.Helper()

	volsOut, err := client.DescribeVolumes(ctx, &ec2svc.DescribeVolumesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("attachment.instance-id"), Values: []string{instanceID}},
		},
	})
	require.NoError(t, err, "DescribeVolumes should succeed")
	require.NotEmpty(t, volsOut.Volumes, "aws_volume_attachment should attach the volume to the instance")

	var volumeID string
	for _, v := range volsOut.Volumes {
		for _, att := range v.Attachments {
			if aws.ToString(att.Device) == "/dev/sdh" {
				volumeID = aws.ToString(v.VolumeId)
			}
		}
	}

	require.NotEmpty(t, volumeID, "volume attached at /dev/sdh should exist")

	// Filtered by tag:Name, not volume-id: aws_ebs_snapshot_copy.example also
	// carries the source snapshot's VolumeId (real AWS CopySnapshot behavior),
	// so a volume-id filter ambiguously matches both snapshots and picking
	// Snapshots[0] flakes on backend map iteration order.
	snapsOut, err := client.DescribeSnapshots(ctx, &ec2svc.DescribeSnapshotsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-11-snapshot"}},
		},
	})
	require.NoError(t, err, "DescribeSnapshots should succeed")
	require.Len(t, snapsOut.Snapshots, 1)
	snapshotID := aws.ToString(snapsOut.Snapshots[0].SnapshotId)

	permOut, err := client.DescribeSnapshotAttribute(ctx, &ec2svc.DescribeSnapshotAttributeInput{
		SnapshotId: aws.String(snapshotID),
		Attribute:  ec2types.SnapshotAttributeNameCreateVolumePermission,
	})
	require.NoError(t, err, "DescribeSnapshotAttribute should succeed")

	var foundPermission bool

	for _, p := range permOut.CreateVolumePermissions {
		if aws.ToString(p.UserId) == "123456789012" {
			foundPermission = true
		}
	}

	assert.True(t, foundPermission, "snapshot create volume permission should be granted")

	imgsOut, err := client.DescribeImages(ctx, &ec2svc.DescribeImagesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("name"), Values: []string{"mega-batch-11-ami"}},
		},
	})
	require.NoError(t, err, "DescribeImages should succeed")
	require.Len(t, imgsOut.Images, 1)
	amiID := aws.ToString(imgsOut.Images[0].ImageId)

	laOut, err := client.DescribeImageAttribute(ctx, &ec2svc.DescribeImageAttributeInput{
		ImageId:   aws.String(amiID),
		Attribute: ec2types.ImageAttributeNameLaunchPermission,
	})
	require.NoError(t, err, "DescribeImageAttribute should succeed")

	var foundLaunchPerm bool

	for _, lp := range laOut.LaunchPermissions {
		if aws.ToString(lp.UserId) == "123456789012" {
			foundLaunchPerm = true
		}
	}

	assert.True(t, foundLaunchPerm, "AMI launch permission should be granted")

	copyOut, err := client.DescribeImages(ctx, &ec2svc.DescribeImagesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("name"), Values: []string{"mega-batch-11-ami-copy"}},
		},
	})
	require.NoError(t, err, "DescribeImages for the AMI copy should succeed")
	require.Len(t, copyOut.Images, 1)

	snapCopyOut, err := client.DescribeSnapshots(ctx, &ec2svc.DescribeSnapshotsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-11-snapshot-copy"}},
		},
	})
	require.NoError(t, err, "DescribeSnapshots for the snapshot copy should succeed")
	require.Len(t, snapCopyOut.Snapshots, 1)
}

// verifyMegaBatch11AccountSettings checks the account-wide EBS/AMI/serial
// console settings singletons.
func verifyMegaBatch11AccountSettings(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	kmsKeyOut, err := client.GetEbsDefaultKmsKeyId(ctx, &ec2svc.GetEbsDefaultKmsKeyIdInput{})
	require.NoError(t, err, "GetEbsDefaultKmsKeyId should succeed")
	assert.NotEmpty(t, aws.ToString(kmsKeyOut.KmsKeyId))

	encOut, err := client.GetEbsEncryptionByDefault(ctx, &ec2svc.GetEbsEncryptionByDefaultInput{})
	require.NoError(t, err, "GetEbsEncryptionByDefault should succeed")
	assert.True(t, aws.ToBool(encOut.EbsEncryptionByDefault))

	blockOut, err := client.GetImageBlockPublicAccessState(ctx, &ec2svc.GetImageBlockPublicAccessStateInput{})
	require.NoError(t, err, "GetImageBlockPublicAccessState should succeed")
	assert.Equal(t, "block-new-sharing", aws.ToString(blockOut.ImageBlockPublicAccessState))

	serialOut, err := client.GetSerialConsoleAccessStatus(ctx, &ec2svc.GetSerialConsoleAccessStatusInput{})
	require.NoError(t, err, "GetSerialConsoleAccessStatus should succeed")
	assert.True(t, aws.ToBool(serialOut.SerialConsoleAccessEnabled))

	azgOut, err := client.DescribeAvailabilityZones(ctx, &ec2svc.DescribeAvailabilityZonesInput{
		AllAvailabilityZones: aws.Bool(true),
		Filters: []ec2types.Filter{
			{Name: aws.String("group-name"), Values: []string{"us-east-1-wl1-bos-wlz-1"}},
		},
	})
	require.NoError(t, err, "DescribeAvailabilityZones should succeed")

	var foundOptedIn bool

	for _, az := range azgOut.AvailabilityZones {
		if az.OptInStatus == ec2types.AvailabilityZoneOptInStatusOptedIn {
			foundOptedIn = true
		}
	}

	assert.True(t, foundOptedIn, "availability zone group should be opted in")
}

// verifyMegaBatch11InstanceState waits for aws_ec2_instance_state to have
// stopped the given instance.
func verifyMegaBatch11InstanceState(ctx context.Context, t *testing.T, client *ec2svc.Client, instanceID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, pollErr := client.DescribeInstances(ctx, &ec2svc.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		})
		if pollErr != nil || len(out.Reservations) != 1 || len(out.Reservations[0].Instances) != 1 {
			return false
		}

		return out.Reservations[0].Instances[0].State.Name == ec2types.InstanceStateNameStopped
	}, 10*time.Second, 100*time.Millisecond, "aws_ec2_instance_state should stop the instance")
}

// verifyMegaBatch11PrefixListAndTag checks the managed prefix list, its
// entry, and the standalone aws_ec2_tag on the VPC.
func verifyMegaBatch11PrefixListAndTag(ctx context.Context, t *testing.T, client *ec2svc.Client, vpcID string) {
	t.Helper()

	plOut, err := client.DescribeManagedPrefixLists(ctx, &ec2svc.DescribeManagedPrefixListsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("prefix-list-name"), Values: []string{"mega-batch-11-pl"}},
		},
	})
	require.NoError(t, err, "DescribeManagedPrefixLists should succeed")
	require.Len(t, plOut.PrefixLists, 1)
	prefixListID := aws.ToString(plOut.PrefixLists[0].PrefixListId)

	entriesOut, err := client.GetManagedPrefixListEntries(ctx, &ec2svc.GetManagedPrefixListEntriesInput{
		PrefixListId: aws.String(prefixListID),
	})
	require.NoError(t, err, "GetManagedPrefixListEntries should succeed")

	var foundEntry bool

	for _, e := range entriesOut.Entries {
		if aws.ToString(e.Cidr) == "10.99.0.0/24" {
			foundEntry = true
		}
	}

	assert.True(t, foundEntry, "managed prefix list entry should be present")

	tagsOut, err := client.DescribeTags(ctx, &ec2svc.DescribeTagsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("resource-id"), Values: []string{vpcID}},
			{Name: aws.String("key"), Values: []string{"mega-batch-11-tag"}},
		},
	})
	require.NoError(t, err, "DescribeTags should succeed")
	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "true", aws.ToString(tagsOut.Tags[0].Value))
}

// verifyMegaBatch11FlowLogAndENI checks the VPC flow log and the network
// interface's instance attachment and security-group attachment.
func verifyMegaBatch11FlowLogAndENI(ctx context.Context, t *testing.T, client *ec2svc.Client, vpcID string) {
	t.Helper()

	flOut, err := client.DescribeFlowLogs(ctx, &ec2svc.DescribeFlowLogsInput{
		Filter: []ec2types.Filter{
			{Name: aws.String("resource-id"), Values: []string{vpcID}},
		},
	})
	require.NoError(t, err, "DescribeFlowLogs should succeed")
	require.Len(t, flOut.FlowLogs, 1)
	assert.Equal(t, ec2types.TrafficTypeAll, flOut.FlowLogs[0].TrafficType)

	eniOut, err := client.DescribeNetworkInterfaces(ctx, &ec2svc.DescribeNetworkInterfacesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-11-eni"}},
		},
	})
	require.NoError(t, err, "DescribeNetworkInterfaces should succeed")
	require.Len(t, eniOut.NetworkInterfaces, 1)
	eni := eniOut.NetworkInterfaces[0]
	require.NotNil(t, eni.Attachment, "aws_network_interface_attachment should attach the ENI to an instance")
	assert.EqualValues(t, 1, aws.ToInt32(eni.Attachment.DeviceIndex))

	var foundSGAttachment bool

	for _, g := range eni.Groups {
		if aws.ToString(g.GroupName) == "mega-batch-11-sg" {
			foundSGAttachment = true
		}
	}

	assert.True(t, foundSGAttachment, "aws_network_interface_sg_attachment should attach the security group")
}

// verifyMegaBatch11SpotAndFleet checks the spot instance request and EC2
// fleet. aws_spot_fleet_request is deliberately not covered here -- see
// PARITY.md's items_still_open entry for why.
func verifyMegaBatch11SpotAndFleet(ctx context.Context, t *testing.T, client *ec2svc.Client) {
	t.Helper()

	spotOut, err := client.DescribeSpotInstanceRequests(ctx, &ec2svc.DescribeSpotInstanceRequestsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Name"), Values: []string{"mega-batch-11-spot"}},
		},
	})
	require.NoError(t, err, "DescribeSpotInstanceRequests should succeed")
	require.Len(t, spotOut.SpotInstanceRequests, 1)

	fleetOut, err := client.DescribeFleets(ctx, &ec2svc.DescribeFleetsInput{})
	require.NoError(t, err, "DescribeFleets should succeed")

	var foundFleet bool

	for _, f := range fleetOut.Fleets {
		if f.Type == ec2types.FleetTypeMaintain {
			foundFleet = true
		}
	}

	assert.True(t, foundFleet, "ec2 fleet should be listed")
}
