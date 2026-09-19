package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_CapacityTransitLaunchTemplate proves fixes to tier-1
// reqfielddiff findings (gopherstack-xhu2t/99nj, ec2 filter+field sweep
// 2026-09-13): Capacity Reservation filters/InstanceMatchCriteria/Tenancy,
// interruptible allocation ZeroSizePreference, Transit Gateway and Transit
// Gateway Route Table filters, Transit Gateway Connect Peer's
// TransitGatewayAddress, launch template version filters, and
// CreateLaunchTemplateVersion's SourceVersion inheritance.
func TestRealClient_CapacityTransitLaunchTemplate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *ec2sdk.Client)
		name string
	}{
		{runCapacityReservationCreateFields, "capacity_reservation_create_fields"},
		{runCapacityReservationDescribeFilters, "capacity_reservation_describe_filters"},
		{runInterruptibleAllocationZeroSizePreference, "interruptible_allocation_zero_size_preference"},
		{runTransitGatewayDescribeFilters, "transit_gateway_describe_filters"},
		{runTransitGatewayRouteTableDescribeFilters, "transit_gateway_route_table_describe_filters"},
		{runTransitGatewayConnectPeerAddress, "transit_gateway_connect_peer_address"},
		{runLaunchTemplateVersionFilters, "launch_template_version_filters"},
		{runLaunchTemplateVersionSourceVersion, "launch_template_version_source_version"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestEC2Client(t, h)
			tt.run(t, client)
		})
	}
}

func runCapacityReservationCreateFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	created, err := client.CreateCapacityReservation(t.Context(), &ec2sdk.CreateCapacityReservationInput{
		InstanceType:          aws.String("m5.large"),
		InstancePlatform:      types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone:      aws.String("us-east-1a"),
		InstanceCount:         aws.Int32(2),
		InstanceMatchCriteria: types.InstanceMatchCriteriaTargeted,
		Tenancy:               types.CapacityReservationTenancyDedicated,
	})
	require.NoError(t, err)

	assert.Equal(t, types.InstanceMatchCriteriaTargeted, created.CapacityReservation.InstanceMatchCriteria)
	assert.Equal(t, types.CapacityReservationTenancyDedicated, created.CapacityReservation.Tenancy)
}

func runCapacityReservationDescribeFilters(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	_, err := client.CreateCapacityReservation(t.Context(), &ec2sdk.CreateCapacityReservationInput{
		InstanceType:      aws.String("m5.large"),
		InstancePlatform:  types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone:  aws.String("us-east-1a"),
		InstanceCount:     aws.Int32(1),
		Tenancy:           types.CapacityReservationTenancyDefault,
		TagSpecifications: nil,
	})
	require.NoError(t, err)

	dedicated, err := client.CreateCapacityReservation(t.Context(), &ec2sdk.CreateCapacityReservationInput{
		InstanceType:     aws.String("c5.xlarge"),
		InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceCount:    aws.Int32(1),
		Tenancy:          types.CapacityReservationTenancyDedicated,
	})
	require.NoError(t, err)

	out, err := client.DescribeCapacityReservations(t.Context(), &ec2sdk.DescribeCapacityReservationsInput{
		Filters: []types.Filter{{Name: aws.String("tenancy"), Values: []string{"dedicated"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.CapacityReservations, 1, "tenancy filter must narrow to only the dedicated reservation")
	assert.Equal(
		t, aws.ToString(dedicated.CapacityReservation.CapacityReservationId),
		aws.ToString(out.CapacityReservations[0].CapacityReservationId),
	)
}

func runInterruptibleAllocationZeroSizePreference(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	src, err := client.CreateCapacityReservation(t.Context(), &ec2sdk.CreateCapacityReservationInput{
		InstanceType:     aws.String("m5.large"),
		InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceCount:    aws.Int32(10),
	})
	require.NoError(t, err)
	crID := src.CapacityReservation.CapacityReservationId

	_, err = client.CreateInterruptibleCapacityReservationAllocation(
		t.Context(), &ec2sdk.CreateInterruptibleCapacityReservationAllocationInput{
			CapacityReservationId: crID,
			InstanceCount:         aws.Int32(4),
			ZeroSizePreference:    types.ZeroSizePreferenceRetain,
		},
	)
	require.NoError(t, err)

	retained, err := client.UpdateInterruptibleCapacityReservationAllocation(
		t.Context(), &ec2sdk.UpdateInterruptibleCapacityReservationAllocationInput{
			CapacityReservationId: crID,
			TargetInstanceCount:   aws.Int32(0),
			ZeroSizePreference:    types.ZeroSizePreferenceRetain,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t, "active", string(retained.Status),
		"ZeroSizePreference=retain must keep the allocation active at zero capacity",
	)

	canceled, err := client.UpdateInterruptibleCapacityReservationAllocation(
		t.Context(), &ec2sdk.UpdateInterruptibleCapacityReservationAllocationInput{
			CapacityReservationId: crID,
			TargetInstanceCount:   aws.Int32(0),
			ZeroSizePreference:    types.ZeroSizePreferenceDefault,
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t, "canceled", string(canceled.Status),
		"ZeroSizePreference=default must cancel the allocation at zero capacity",
	)
}

func runTransitGatewayDescribeFilters(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	enabled, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{
		Options: &types.TransitGatewayRequestOptions{DnsSupport: types.DnsSupportValueEnable},
	})
	require.NoError(t, err)

	_, err = client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{
		Options: &types.TransitGatewayRequestOptions{DnsSupport: types.DnsSupportValueDisable},
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGateways(t.Context(), &ec2sdk.DescribeTransitGatewaysInput{
		Filters: []types.Filter{{Name: aws.String("options.dns-support"), Values: []string{"enable"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.TransitGateways, 1, "options.dns-support filter must narrow to only the enabled gateway")
	assert.Equal(
		t, aws.ToString(enabled.TransitGateway.TransitGatewayId),
		aws.ToString(out.TransitGateways[0].TransitGatewayId),
	)
}

func runTransitGatewayRouteTableDescribeFilters(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	tgwA, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)
	tgwB, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	rtA, err := client.CreateTransitGatewayRouteTable(t.Context(), &ec2sdk.CreateTransitGatewayRouteTableInput{
		TransitGatewayId: tgwA.TransitGateway.TransitGatewayId,
	})
	require.NoError(t, err)

	_, err = client.CreateTransitGatewayRouteTable(t.Context(), &ec2sdk.CreateTransitGatewayRouteTableInput{
		TransitGatewayId: tgwB.TransitGateway.TransitGatewayId,
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayRouteTables(t.Context(), &ec2sdk.DescribeTransitGatewayRouteTablesInput{
		Filters: []types.Filter{{
			Name:   aws.String("transit-gateway-id"),
			Values: []string{aws.ToString(tgwA.TransitGateway.TransitGatewayId)},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayRouteTables, 1, "transit-gateway-id filter must narrow to tgwA's route table")
	assert.Equal(
		t, aws.ToString(rtA.TransitGatewayRouteTable.TransitGatewayRouteTableId),
		aws.ToString(out.TransitGatewayRouteTables[0].TransitGatewayRouteTableId),
	)
}

func runTransitGatewayConnectPeerAddress(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{
		Options: &types.TransitGatewayRequestOptions{TransitGatewayCidrBlocks: []string{"10.100.0.0/24"}},
	})
	require.NoError(t, err)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.37.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.37.1.0/24"),
	})
	require.NoError(t, err)

	vpcAtt, err := client.CreateTransitGatewayVpcAttachment(
		t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
			TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
			VpcId:            vpc.Vpc.VpcId,
			SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
		},
	)
	require.NoError(t, err)

	connect, err := client.CreateTransitGatewayConnect(t.Context(), &ec2sdk.CreateTransitGatewayConnectInput{
		TransportTransitGatewayAttachmentId: vpcAtt.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
		Options: &types.CreateTransitGatewayConnectRequestOptions{
			Protocol: types.ProtocolValueGre,
		},
	})
	require.NoError(t, err)

	overridden, err := client.CreateTransitGatewayConnectPeer(
		t.Context(), &ec2sdk.CreateTransitGatewayConnectPeerInput{
			TransitGatewayAttachmentId: connect.TransitGatewayConnect.TransitGatewayAttachmentId,
			PeerAddress:                aws.String("192.0.2.10"),
			InsideCidrBlocks:           []string{"169.254.100.0/29"},
			TransitGatewayAddress:      aws.String("10.100.0.55"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, overridden.TransitGatewayConnectPeer.ConnectPeerConfiguration)
	assert.Equal(
		t, "10.100.0.55",
		aws.ToString(overridden.TransitGatewayConnectPeer.ConnectPeerConfiguration.TransitGatewayAddress),
		"explicit TransitGatewayAddress must round-trip",
	)

	autoAssigned, err := client.CreateTransitGatewayConnectPeer(
		t.Context(), &ec2sdk.CreateTransitGatewayConnectPeerInput{
			TransitGatewayAttachmentId: connect.TransitGatewayConnect.TransitGatewayAttachmentId,
			PeerAddress:                aws.String("192.0.2.20"),
			InsideCidrBlocks:           []string{"169.254.101.0/29"},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, autoAssigned.TransitGatewayConnectPeer.ConnectPeerConfiguration)
	assert.Equal(
		t, "10.100.0.1",
		aws.ToString(autoAssigned.TransitGatewayConnectPeer.ConnectPeerConfiguration.TransitGatewayAddress),
		"omitted TransitGatewayAddress must auto-assign the first host address of the TGW's CIDR block",
	)
}

func runLaunchTemplateVersionFilters(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	created, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("filter-test-lt"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-11112222"),
			InstanceType: types.InstanceTypeT3Small,
		},
	})
	require.NoError(t, err)
	ltID := created.LaunchTemplate.LaunchTemplateId

	out, err := client.DescribeLaunchTemplateVersions(t.Context(), &ec2sdk.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateId: ltID,
		Filters:          []types.Filter{{Name: aws.String("image-id"), Values: []string{"ami-does-not-exist"}}},
	})
	require.NoError(t, err)
	assert.Empty(t, out.LaunchTemplateVersions, "image-id filter with no match must return no versions")

	out2, err := client.DescribeLaunchTemplateVersions(t.Context(), &ec2sdk.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateId: ltID,
		Filters:          []types.Filter{{Name: aws.String("image-id"), Values: []string{"ami-11112222"}}},
	})
	require.NoError(t, err)
	require.Len(t, out2.LaunchTemplateVersions, 1, "image-id filter matching the seeded version must return it")
}

func runLaunchTemplateVersionSourceVersion(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	created, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("source-version-test-lt"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-v1"),
			InstanceType: types.InstanceTypeT3Micro,
		},
	})
	require.NoError(t, err)
	ltID := created.LaunchTemplate.LaunchTemplateId

	_, err = client.CreateLaunchTemplateVersion(t.Context(), &ec2sdk.CreateLaunchTemplateVersionInput{
		LaunchTemplateId: ltID,
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			ImageId:      aws.String("ami-v2"),
			InstanceType: types.InstanceTypeT3Small,
		},
	})
	require.NoError(t, err)

	v3, err := client.CreateLaunchTemplateVersion(t.Context(), &ec2sdk.CreateLaunchTemplateVersionInput{
		LaunchTemplateId: ltID,
		SourceVersion:    aws.String("1"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{
			InstanceType: types.InstanceTypeT3Large,
		},
	})
	require.NoError(t, err)

	require.NotNil(t, v3.LaunchTemplateVersion.LaunchTemplateData)
	assert.Equal(
		t, "ami-v1", aws.ToString(v3.LaunchTemplateVersion.LaunchTemplateData.ImageId),
		"SourceVersion=1 must inherit ImageId from version 1, not the just-created version 2",
	)
	assert.Equal(
		t, types.InstanceTypeT3Large, v3.LaunchTemplateVersion.LaunchTemplateData.InstanceType,
		"an explicit field must still override the inherited source version",
	)
}

// TestRealClient_ReservedInstancesOfferingFilters proves
// DescribeReservedInstancesOfferings now applies Filters, InstanceTenancy,
// MinDuration, and MaxDuration instead of ignoring them (reqfielddiff
// tier-1, 2026-09-13).
func TestRealClient_ReservedInstancesOfferingFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	backend.SeedReservedInstancesOffering(
		"rio-default-short", "t3.micro", "us-east-1a", "Linux/UNIX", "No Upfront", "standard",
		31536000, 0, 0.05, "default",
	)
	backend.SeedReservedInstancesOffering(
		"rio-dedicated-long", "t3.micro", "us-east-1a", "Linux/UNIX", "No Upfront", "standard",
		94608000, 0, 0.09, "dedicated",
	)

	t.Run("instance_tenancy", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeReservedInstancesOfferings(
			t.Context(), &ec2sdk.DescribeReservedInstancesOfferingsInput{
				InstanceTenancy: types.TenancyDedicated,
			},
		)
		require.NoError(t, err)
		require.Len(t, out.ReservedInstancesOfferings, 1)
		assert.Equal(
			t, "rio-dedicated-long", aws.ToString(out.ReservedInstancesOfferings[0].ReservedInstancesOfferingId),
		)
		assert.Equal(t, types.TenancyDedicated, out.ReservedInstancesOfferings[0].InstanceTenancy)
	})

	t.Run("min_and_max_duration", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeReservedInstancesOfferings(
			t.Context(), &ec2sdk.DescribeReservedInstancesOfferingsInput{
				MinDuration: aws.Int64(50000000),
				MaxDuration: aws.Int64(94608000),
			},
		)
		require.NoError(t, err)
		require.Len(t, out.ReservedInstancesOfferings, 1)
		assert.Equal(
			t, "rio-dedicated-long", aws.ToString(out.ReservedInstancesOfferings[0].ReservedInstancesOfferingId),
		)
	})

	t.Run("filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeReservedInstancesOfferings(
			t.Context(), &ec2sdk.DescribeReservedInstancesOfferingsInput{
				Filters: []types.Filter{{Name: aws.String("usage-price"), Values: []string{"0.09"}}},
			},
		)
		require.NoError(t, err)
		require.Len(t, out.ReservedInstancesOfferings, 1)
		assert.Equal(
			t, "rio-dedicated-long", aws.ToString(out.ReservedInstancesOfferings[0].ReservedInstancesOfferingId),
		)
	})
}
