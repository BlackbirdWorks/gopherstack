package ec2_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestDescribeVpcPeeringConnections_VpcInfoFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter types.Filter
		want   int
	}{
		{name: "requester cidr", filter: wireFilter("requester-vpc-info.cidr-block", "10.0.0.0/16"), want: 1},
		{name: "accepter cidr", filter: wireFilter("accepter-vpc-info.cidr-block", "10.1.0.0/16"), want: 1},
		{name: "wrong cidr", filter: wireFilter("requester-vpc-info.cidr-block", "10.1.0.0/16"), want: 0},
		{name: "requester owner", filter: wireFilter("requester-vpc-info.owner-id", "123456789012"), want: 1},
		{name: "other owner", filter: wireFilter("requester-vpc-info.owner-id", "999999999999"), want: 0},
		{name: "expiration wildcard", filter: wireFilter("expiration-time", "20*"), want: 1},
		{name: "expiration other", filter: wireFilter("expiration-time", "1999-*"), want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)
			ctx := t.Context()

			a, err := client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
			require.NoError(t, err)
			b, err := client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.1.0.0/16")})
			require.NoError(t, err)

			created, err := client.CreateVpcPeeringConnection(ctx, &ec2sdk.CreateVpcPeeringConnectionInput{
				VpcId: a.Vpc.VpcId, PeerVpcId: b.Vpc.VpcId,
			})
			require.NoError(t, err)

			info := created.VpcPeeringConnection
			assert.Equal(t, "10.0.0.0/16", aws.ToString(info.RequesterVpcInfo.CidrBlock))
			assert.Equal(t, "10.1.0.0/16", aws.ToString(info.AccepterVpcInfo.CidrBlock))
			assert.Equal(t, "123456789012", aws.ToString(info.RequesterVpcInfo.OwnerId))
			assert.NotNil(t, info.ExpirationTime)

			out, err := client.DescribeVpcPeeringConnections(ctx, &ec2sdk.DescribeVpcPeeringConnectionsInput{
				Filters: []types.Filter{tc.filter},
			})
			require.NoError(t, err)
			assert.Len(t, out.VpcPeeringConnections, tc.want)
		})
	}
}

func TestSearchLocalGatewayRoutes_RouteSearchFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		value  string
		want   []string
	}{
		{name: "exact", filter: "route-search.exact-match", value: "10.0.1.0/29", want: []string{"10.0.1.0/29"}},
		{
			name:   "subnet of",
			filter: "route-search.subnet-of-match",
			value:  "10.0.1.0/28",
			want:   []string{"10.0.1.0/29", "10.0.1.0/31"},
		},
		{
			name:   "supernet of",
			filter: "route-search.supernet-of-match",
			value:  "10.0.1.0/30",
			want:   []string{"10.0.1.0/29"},
		},
		{
			name:   "longest prefix",
			filter: "route-search.longest-prefix-match",
			value:  "10.0.1.0/32",
			want:   []string{"10.0.1.0/31"},
		},
		{name: "longest prefix none", filter: "route-search.longest-prefix-match", value: "192.168.0.1/32", want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestEC2Client(t, ec2.NewHandler(b))
			ctx := t.Context()

			lg, err := b.SeedLocalGateway(
				ec2.LocalGateway{OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1"},
			)
			require.NoError(t, err)

			rt, err := client.CreateLocalGatewayRouteTable(ctx, &ec2sdk.CreateLocalGatewayRouteTableInput{
				LocalGatewayId: aws.String(lg.LocalGatewayID),
			})
			require.NoError(t, err)

			for _, cidr := range []string{"10.0.1.0/29", "10.0.1.0/31", "172.16.0.0/16"} {
				_, err = client.CreateLocalGatewayRoute(ctx, &ec2sdk.CreateLocalGatewayRouteInput{
					LocalGatewayRouteTableId: rt.LocalGatewayRouteTable.LocalGatewayRouteTableId,
					DestinationCidrBlock:     aws.String(cidr),
				})
				require.NoError(t, err)
			}

			out, err := client.SearchLocalGatewayRoutes(ctx, &ec2sdk.SearchLocalGatewayRoutesInput{
				LocalGatewayRouteTableId: rt.LocalGatewayRouteTable.LocalGatewayRouteTableId,
				Filters:                  []types.Filter{wireFilter(tc.filter, tc.value)},
			})
			require.NoError(t, err)

			var got []string
			for _, r := range out.Routes {
				got = append(got, aws.ToString(r.DestinationCidrBlock))
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestSearchTransitGatewayRoutes_RouteSearchFilters(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)
	ctx := t.Context()

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	rt, err := client.CreateTransitGatewayRouteTable(ctx, &ec2sdk.CreateTransitGatewayRouteTableInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
	})
	require.NoError(t, err)

	_ = b

	for _, cidr := range []string{"10.0.1.0/29", "10.0.1.0/31"} {
		_, err = client.CreateTransitGatewayRoute(ctx, &ec2sdk.CreateTransitGatewayRouteInput{
			TransitGatewayRouteTableId: rt.TransitGatewayRouteTable.TransitGatewayRouteTableId,
			DestinationCidrBlock:       aws.String(cidr),
			Blackhole:                  aws.Bool(true),
		})
		require.NoError(t, err)
	}

	out, err := client.SearchTransitGatewayRoutes(ctx, &ec2sdk.SearchTransitGatewayRoutesInput{
		TransitGatewayRouteTableId: rt.TransitGatewayRouteTable.TransitGatewayRouteTableId,
		Filters:                    []types.Filter{wireFilter("route-search.supernet-of-match", "10.0.1.0/30")},
	})
	require.NoError(t, err)
	require.Len(t, out.Routes, 1)
	assert.Equal(t, "10.0.1.0/29", aws.ToString(out.Routes[0].DestinationCidrBlock))
}

func TestDescribeInstanceStatus_ScheduledEventAndOperatorFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter types.Filter
		want   int
	}{
		{name: "event code", filter: wireFilter("event.code", "instance-reboot"), want: 0},
		{name: "operator managed false", filter: wireFilter("operator.managed", "false"), want: 1},
		{name: "operator managed true", filter: wireFilter("operator.managed", "true"), want: 0},
		{name: "operator principal", filter: wireFilter("operator.principal", "x"), want: 0},
		{name: "attached ebs ok", filter: wireFilter("attached-ebs-status.status", "ok"), want: 1},
		{name: "attached ebs impaired", filter: wireFilter("attached-ebs-status.status", "impaired"), want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, client := newTestBackendAndClient(t)
			ctx := t.Context()

			run, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
				ImageId: aws.String("ami-12345678"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
			})
			require.NoError(t, err)
			require.Len(t, run.Instances, 1)

			b.TickLifecycleForTest()

			out, err := client.DescribeInstanceStatus(ctx, &ec2sdk.DescribeInstanceStatusInput{
				InstanceIds: []string{aws.ToString(run.Instances[0].InstanceId)},
				Filters:     []types.Filter{tc.filter},
			})
			require.NoError(t, err)
			assert.Len(t, out.InstanceStatuses, tc.want)

			for _, st := range out.InstanceStatuses {
				require.NotNil(t, st.AttachedEbsStatus)
				assert.Equal(t, types.SummaryStatusOk, st.AttachedEbsStatus.Status)
			}
		})
	}
}

func TestDescribeInstanceImageMetadata_OwnerAlias(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)
	ctx := t.Context()

	amazon := firstAmazonImageID(t, client)
	run, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId: aws.String(amazon), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)

	b.TickLifecycleForTest()

	out, err := client.DescribeInstanceImageMetadata(ctx, &ec2sdk.DescribeInstanceImageMetadataInput{
		Filters: []types.Filter{wireFilter("owner-alias", "amazon")},
	})
	require.NoError(t, err)
	require.Len(t, out.InstanceImageMetadata, 1)
	assert.Equal(t, aws.ToString(run.Instances[0].InstanceId), aws.ToString(out.InstanceImageMetadata[0].InstanceId))
	assert.Equal(t, "amazon", aws.ToString(out.InstanceImageMetadata[0].ImageMetadata.ImageOwnerAlias))

	none, err := client.DescribeInstanceImageMetadata(ctx, &ec2sdk.DescribeInstanceImageMetadataInput{
		Filters: []types.Filter{wireFilter("owner-alias", "aws-marketplace")},
	})
	require.NoError(t, err)
	assert.Empty(t, none.InstanceImageMetadata)
}

func firstAmazonImageID(t *testing.T, client *ec2sdk.Client) string {
	t.Helper()

	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{Owners: []string{"amazon"}})
	require.NoError(t, err)
	require.NotEmpty(t, out.Images)

	for _, img := range out.Images {
		if strings.HasPrefix(aws.ToString(img.ImageId), "ami-") {
			return aws.ToString(img.ImageId)
		}
	}

	return aws.ToString(out.Images[0].ImageId)
}

func TestDescribeReservedInstancesModifications_TokenAndDates(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))
	ctx := t.Context()

	b.SeedReservedInstancesOffering("rio-1", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", "standard",
		94608000, 500.0, 0.0)

	ri, err := b.PurchaseReservedInstancesOffering("rio-1", 2)
	require.NoError(t, err)

	in := &ec2sdk.ModifyReservedInstancesInput{
		ClientToken:          aws.String("tok-1"),
		ReservedInstancesIds: []string{ri.ReservedInstancesID},
		TargetConfigurations: []types.ReservedInstancesConfiguration{
			{
				InstanceType:     types.InstanceTypeT3Large,
				InstanceCount:    aws.Int32(2),
				AvailabilityZone: aws.String("us-east-1b"),
			},
		},
	}

	first, err := client.ModifyReservedInstances(ctx, in)
	require.NoError(t, err)

	second, err := client.ModifyReservedInstances(ctx, in)
	require.NoError(t, err)
	assert.Equal(
		t,
		aws.ToString(first.ReservedInstancesModificationId),
		aws.ToString(second.ReservedInstancesModificationId),
		"a replayed ClientToken returns the original modification",
	)

	mods, err := client.DescribeReservedInstancesModifications(ctx, &ec2sdk.DescribeReservedInstancesModificationsInput{
		Filters: []types.Filter{wireFilter("client-token", "tok-1"), wireFilter("create-date", "20*")},
	})
	require.NoError(t, err)
	require.Len(t, mods.ReservedInstancesModifications, 1)

	mod := mods.ReservedInstancesModifications[0]
	assert.Equal(t, "tok-1", aws.ToString(mod.ClientToken))
	assert.NotNil(t, mod.CreateDate)
	assert.NotNil(t, mod.EffectiveDate)
	require.Len(t, mod.ModificationResults, 1)

	minted := aws.ToString(mod.ModificationResults[0].ReservedInstancesId)
	require.NotEmpty(t, minted)

	byMinted, err := client.DescribeReservedInstancesModifications(
		ctx,
		&ec2sdk.DescribeReservedInstancesModificationsInput{
			Filters: []types.Filter{wireFilter("modification-result.reserved-instances-id", minted)},
		},
	)
	require.NoError(t, err)
	assert.Len(t, byMinted.ReservedInstancesModifications, 1)

	other, err := client.DescribeReservedInstancesModifications(
		ctx,
		&ec2sdk.DescribeReservedInstancesModificationsInput{
			Filters: []types.Filter{wireFilter("client-token", "nope")},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, other.ReservedInstancesModifications)
}

func TestSearchTransitGatewayMulticastGroups_PlacementFilters(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)
	ctx := t.Context()

	vpc, err := client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(ctx, &ec2sdk.CreateSubnetInput{
		VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.0.1.0/24"),
	})
	require.NoError(t, err)
	eni, err := client.CreateNetworkInterface(
		ctx,
		&ec2sdk.CreateNetworkInterfaceInput{SubnetId: subnet.Subnet.SubnetId},
	)
	require.NoError(t, err)

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)
	att, err := client.CreateTransitGatewayVpcAttachment(ctx, &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
		VpcId:            vpc.Vpc.VpcId,
		SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
	})
	require.NoError(t, err)

	dom, err := client.CreateTransitGatewayMulticastDomain(ctx, &ec2sdk.CreateTransitGatewayMulticastDomainInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
	})
	require.NoError(t, err)

	domainID := dom.TransitGatewayMulticastDomain.TransitGatewayMulticastDomainId
	attachmentID := att.TransitGatewayVpcAttachment.TransitGatewayAttachmentId

	_, err = client.AssociateTransitGatewayMulticastDomain(ctx, &ec2sdk.AssociateTransitGatewayMulticastDomainInput{
		TransitGatewayMulticastDomainId: domainID,
		TransitGatewayAttachmentId:      attachmentID,
		SubnetIds:                       []string{aws.ToString(subnet.Subnet.SubnetId)},
	})
	require.NoError(t, err)

	_, err = client.RegisterTransitGatewayMulticastGroupMembers(ctx,
		&ec2sdk.RegisterTransitGatewayMulticastGroupMembersInput{
			TransitGatewayMulticastDomainId: domainID,
			GroupIpAddress:                  aws.String("224.0.1.1"),
			NetworkInterfaceIds:             []string{aws.ToString(eni.NetworkInterface.NetworkInterfaceId)},
		})
	require.NoError(t, err)

	tests := []struct {
		name   string
		filter types.Filter
		want   int
	}{
		{name: "subnet", filter: wireFilter("subnet-id", aws.ToString(subnet.Subnet.SubnetId)), want: 1},
		{name: "other subnet", filter: wireFilter("subnet-id", "subnet-none"), want: 0},
		{name: "attachment", filter: wireFilter("transit-gateway-attachment-id", aws.ToString(attachmentID)), want: 1},
		{name: "other attachment", filter: wireFilter("transit-gateway-attachment-id", "tgw-attach-none"), want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, searchErr := client.SearchTransitGatewayMulticastGroups(
				ctx,
				&ec2sdk.SearchTransitGatewayMulticastGroupsInput{
					TransitGatewayMulticastDomainId: domainID,
					Filters:                         []types.Filter{tc.filter},
				},
			)
			require.NoError(t, searchErr)
			require.Len(t, out.MulticastGroups, tc.want)

			if tc.want == 1 {
				assert.Equal(t, aws.ToString(subnet.Subnet.SubnetId), aws.ToString(out.MulticastGroups[0].SubnetId))
				assert.Equal(
					t,
					aws.ToString(attachmentID),
					aws.ToString(out.MulticastGroups[0].TransitGatewayAttachmentId),
				)
			}
		})
	}
}

func TestDescribeCapacityBlockExtensionHistory_InstanceFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))
	ctx := t.Context()

	offerings, err := client.DescribeCapacityBlockOfferings(ctx, &ec2sdk.DescribeCapacityBlockOfferingsInput{
		CapacityDurationHours: aws.Int32(24),
		InstanceType:          aws.String("p5.48xlarge"),
		InstanceCount:         aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotEmpty(t, offerings.CapacityBlockOfferings)

	purchased, err := client.PurchaseCapacityBlock(ctx, &ec2sdk.PurchaseCapacityBlockInput{
		CapacityBlockOfferingId: offerings.CapacityBlockOfferings[0].CapacityBlockOfferingId,
		InstancePlatform:        types.CapacityReservationInstancePlatformLinuxUnix,
	})
	require.NoError(t, err)

	reservationID := purchased.CapacityReservation.CapacityReservationId

	extOfferings, err := client.DescribeCapacityBlockExtensionOfferings(
		ctx,
		&ec2sdk.DescribeCapacityBlockExtensionOfferingsInput{
			CapacityReservationId:               reservationID,
			CapacityBlockExtensionDurationHours: aws.Int32(24),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, extOfferings.CapacityBlockExtensionOfferings)

	_, err = client.PurchaseCapacityBlockExtension(ctx, &ec2sdk.PurchaseCapacityBlockExtensionInput{
		CapacityBlockExtensionOfferingId: extOfferings.CapacityBlockExtensionOfferings[0].CapacityBlockExtensionOfferingId,
		CapacityReservationId:            reservationID,
	})
	require.NoError(t, err)

	tests := []struct {
		name   string
		filter types.Filter
		want   int
	}{
		{name: "instance type", filter: wireFilter("instance-type", "p5.48xlarge"), want: 1},
		{name: "other type", filter: wireFilter("instance-type", "t3.micro"), want: 0},
		{name: "zone id", filter: wireFilter("availability-zone-id", "use1-az1"), want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, histErr := client.DescribeCapacityBlockExtensionHistory(
				ctx,
				&ec2sdk.DescribeCapacityBlockExtensionHistoryInput{
					Filters: []types.Filter{tc.filter},
				},
			)
			require.NoError(t, histErr)

			if tc.want >= 0 {
				require.Len(t, out.CapacityBlockExtensions, tc.want)
			}

			for _, e := range out.CapacityBlockExtensions {
				assert.Equal(t, "p5.48xlarge", aws.ToString(e.InstanceType))
				assert.NotEmpty(t, aws.ToString(e.AvailabilityZoneId))
			}
		})
	}
}

func TestDescribeCapacityBlocks_UltraserverTypeFilter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))
	ctx := t.Context()

	offerings, err := client.DescribeCapacityBlockOfferings(ctx, &ec2sdk.DescribeCapacityBlockOfferingsInput{
		CapacityDurationHours: aws.Int32(24),
		InstanceType:          aws.String("p5.48xlarge"),
		InstanceCount:         aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotEmpty(t, offerings.CapacityBlockOfferings)

	_, err = client.PurchaseCapacityBlock(ctx, &ec2sdk.PurchaseCapacityBlockInput{
		CapacityBlockOfferingId: offerings.CapacityBlockOfferings[0].CapacityBlockOfferingId,
		InstancePlatform:        types.CapacityReservationInstancePlatformLinuxUnix,
	})
	require.NoError(t, err)

	instances, err := client.DescribeCapacityBlocks(ctx, &ec2sdk.DescribeCapacityBlocksInput{
		Filters: []types.Filter{wireFilter("ultraserver-type", "instances")},
	})
	require.NoError(t, err)
	assert.Len(t, instances.CapacityBlocks, 1)

	ultra, err := client.DescribeCapacityBlocks(ctx, &ec2sdk.DescribeCapacityBlocksInput{
		Filters: []types.Filter{wireFilter("ultraserver-type", "ultraservers")},
	})
	require.NoError(t, err)
	assert.Empty(t, ultra.CapacityBlocks)
}

func TestDescribeSecondaryInterfaces_AttachmentFilters(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(b))
	ctx := t.Context()

	attached, err := b.SeedSecondaryInterface(ec2.SecondaryInterface{
		InstanceID: "i-1", InstanceOwnerID: "111111111111", PrivateIpv4Addresses: []string{"10.0.0.5"},
	})
	require.NoError(t, err)
	_, err = b.SeedSecondaryInterface(ec2.SecondaryInterface{})
	require.NoError(t, err)

	tests := []struct {
		name   string
		filter types.Filter
		want   int
	}{
		{name: "owner", filter: wireFilter("attachment.instance-owner-id", "111111111111"), want: 1},
		{name: "other owner", filter: wireFilter("attachment.instance-owner-id", "222222222222"), want: 0},
		{name: "attachment id", filter: wireFilter("attachment.attachment-id", attached.AttachmentID), want: 1},
		{name: "status", filter: wireFilter("attachment.status", "attached"), want: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, descErr := client.DescribeSecondaryInterfaces(ctx, &ec2sdk.DescribeSecondaryInterfacesInput{
				Filters: []types.Filter{tc.filter},
			})
			require.NoError(t, descErr)
			assert.Len(t, out.SecondaryInterfaces, tc.want)
		})
	}

	all, err := client.DescribeSecondaryInterfaces(ctx, &ec2sdk.DescribeSecondaryInterfacesInput{
		SecondaryInterfaceIds: []string{attached.SecondaryInterfaceID},
	})
	require.NoError(t, err)
	require.Len(t, all.SecondaryInterfaces, 1)

	si := all.SecondaryInterfaces[0]
	require.NotNil(t, si.Attachment)
	assert.Equal(t, "i-1", aws.ToString(si.Attachment.InstanceId))
	assert.Equal(t, "111111111111", aws.ToString(si.Attachment.InstanceOwnerId))
	require.Len(t, si.PrivateIpv4Addresses, 1)
	assert.Equal(t, "10.0.0.5", aws.ToString(si.PrivateIpv4Addresses[0].PrivateIpAddress))
}
