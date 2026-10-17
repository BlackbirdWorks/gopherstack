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

func TestGeneratedIDShapes(t *testing.T) {
	t.Parallel()

	vpcID := func(t *testing.T, c *ec2sdk.Client) string {
		t.Helper()

		out, err := c.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
		require.NoError(t, err)

		return aws.ToString(out.Vpc.VpcId)
	}

	subnetID := func(t *testing.T, c *ec2sdk.Client) string {
		t.Helper()

		out, err := c.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
			VpcId: aws.String(vpcID(t, c)), CidrBlock: aws.String("10.0.1.0/24"),
		})
		require.NoError(t, err)

		return aws.ToString(out.Subnet.SubnetId)
	}

	tests := []struct {
		mint    func(t *testing.T, c *ec2sdk.Client) string
		name    string
		pattern string
	}{
		{name: "vpc", pattern: `^vpc-[0-9a-f]{17}$`, mint: vpcID},
		{name: "subnet", pattern: `^subnet-[0-9a-f]{17}$`, mint: subnetID},
		{name: "security group", pattern: `^sg-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
				GroupName: aws.String("g"), Description: aws.String("d"), VpcId: aws.String(vpcID(t, c)),
			})
			require.NoError(t, err)

			return aws.ToString(out.GroupId)
		}},
		{name: "internet gateway", pattern: `^igw-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateInternetGateway(t.Context(), &ec2sdk.CreateInternetGatewayInput{})
			require.NoError(t, err)

			return aws.ToString(out.InternetGateway.InternetGatewayId)
		}},
		{name: "route table", pattern: `^rtb-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: aws.String(vpcID(t, c))})
			require.NoError(t, err)

			return aws.ToString(out.RouteTable.RouteTableId)
		}},
		{name: "network acl", pattern: `^acl-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateNetworkAcl(t.Context(), &ec2sdk.CreateNetworkAclInput{VpcId: aws.String(vpcID(t, c))})
			require.NoError(t, err)

			return aws.ToString(out.NetworkAcl.NetworkAclId)
		}},
		{name: "volume", pattern: `^vol-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
				AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8),
			})
			require.NoError(t, err)

			return aws.ToString(out.VolumeId)
		}},
		{name: "snapshot", pattern: `^snap-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			vol, err := c.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
				AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8),
			})
			require.NoError(t, err)

			out, err := c.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: vol.VolumeId})
			require.NoError(t, err)

			return aws.ToString(out.SnapshotId)
		}},
		{name: "elastic ip", pattern: `^eipalloc-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{Domain: types.DomainTypeVpc})
			require.NoError(t, err)

			return aws.ToString(out.AllocationId)
		}},
		{name: "network interface", pattern: `^eni-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
				SubnetId: aws.String(subnetID(t, c)),
			})
			require.NoError(t, err)

			return aws.ToString(out.NetworkInterface.NetworkInterfaceId)
		}},
		{name: "vpc peering", pattern: `^pcx-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateVpcPeeringConnection(t.Context(), &ec2sdk.CreateVpcPeeringConnectionInput{
				VpcId: aws.String(vpcID(t, c)), PeerVpcId: aws.String(vpcID(t, c)),
			})
			require.NoError(t, err)

			return aws.ToString(out.VpcPeeringConnection.VpcPeeringConnectionId)
		}},
		{name: "vpn gateway", pattern: `^vgw-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateVpnGateway(t.Context(), &ec2sdk.CreateVpnGatewayInput{Type: types.GatewayTypeIpsec1})
			require.NoError(t, err)

			return aws.ToString(out.VpnGateway.VpnGatewayId)
		}},
		{name: "customer gateway", pattern: `^cgw-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateCustomerGateway(t.Context(), &ec2sdk.CreateCustomerGatewayInput{
				Type: types.GatewayTypeIpsec1, BgpAsn: aws.Int32(65000), PublicIp: aws.String("203.0.113.1"),
			})
			require.NoError(t, err)

			return aws.ToString(out.CustomerGateway.CustomerGatewayId)
		}},
		{name: "prefix list", pattern: `^pl-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateManagedPrefixList(t.Context(), &ec2sdk.CreateManagedPrefixListInput{
				PrefixListName: aws.String("p"), MaxEntries: aws.Int32(5), AddressFamily: aws.String("IPv4"),
			})
			require.NoError(t, err)

			return aws.ToString(out.PrefixList.PrefixListId)
		}},
		{name: "mirror filter", pattern: `^tmf-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateTrafficMirrorFilter(t.Context(), &ec2sdk.CreateTrafficMirrorFilterInput{})
			require.NoError(t, err)

			return aws.ToString(out.TrafficMirrorFilter.TrafficMirrorFilterId)
		}},
		{name: "carrier gateway", pattern: `^cagw-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateCarrierGateway(t.Context(), &ec2sdk.CreateCarrierGatewayInput{
				VpcId: aws.String(vpcID(t, c)),
			})
			require.NoError(t, err)

			return aws.ToString(out.CarrierGateway.CarrierGatewayId)
		}},
		{name: "ipam", pattern: `^ipam-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateIpam(t.Context(), &ec2sdk.CreateIpamInput{})
			require.NoError(t, err)

			return aws.ToString(out.Ipam.IpamId)
		}},
		{name: "capacity reservation", pattern: `^cr-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.CreateCapacityReservation(t.Context(), &ec2sdk.CreateCapacityReservationInput{
				InstanceType:     aws.String("t3.micro"),
				InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
				AvailabilityZone: aws.String("us-east-1a"),
				InstanceCount:    aws.Int32(1),
			})
			require.NoError(t, err)

			return aws.ToString(out.CapacityReservation.CapacityReservationId)
		}},
		{name: "instance", pattern: `^i-[0-9a-f]{17}$`, mint: func(t *testing.T, c *ec2sdk.Client) string {
			t.Helper()

			out, err := c.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
				ImageId: aws.String("ami-12345678"), InstanceType: types.InstanceTypeT3Micro,
				MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
			})
			require.NoError(t, err)

			return aws.ToString(out.Instances[0].InstanceId)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
			assert.Regexp(t, tt.pattern, tt.mint(t, c))
		})
	}
}
