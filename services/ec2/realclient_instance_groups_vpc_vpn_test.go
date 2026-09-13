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

// TestRealClient_InstanceGroupsVPCVPN proves fixes to tier-1 reqfielddiff
// findings (gopherstack-xhu2t/99nj, ec2 sweep 2026-09-13):
// ModifyInstanceAttribute's Groups, ModifyNetworkInterfaceAttribute's
// Groups, CreateVpcPeeringConnection's PeerOwnerId/PeerRegion, and
// CreateVpnGateway's AmazonSideAsn.
func TestRealClient_InstanceGroupsVPCVPN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *ec2sdk.Client)
		name string
	}{
		{runModifyInstanceAttributeGroups, "modify_instance_attribute_groups"},
		{runModifyNetworkInterfaceAttributeGroups, "modify_network_interface_attribute_groups"},
		{runCreateVpcPeeringConnectionPeerFields, "create_vpc_peering_connection_peer_fields"},
		{runCreateVpnGatewayAmazonSideAsn, "create_vpn_gateway_amazon_side_asn"},
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

func runModifyInstanceAttributeGroups(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.40.0.0/16")})
	require.NoError(t, err)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("modify-instance-attr-sg"),
		Description: aws.String("test sg"),
		VpcId:       vpc.Vpc.VpcId,
	})
	require.NoError(t, err)

	run, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-0c55b159cbfafe1f0"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := run.Instances[0].InstanceId

	_, err = client.ModifyInstanceAttribute(t.Context(), &ec2sdk.ModifyInstanceAttributeInput{
		InstanceId: instanceID,
		Groups:     []string{aws.ToString(sg.GroupId)},
	})
	require.NoError(t, err)

	out, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{aws.ToString(instanceID)},
	})
	require.NoError(t, err)
	require.Len(t, out.Reservations, 1)
	require.Len(t, out.Reservations[0].Instances, 1)
	require.Len(t, out.Reservations[0].Instances[0].SecurityGroups, 1)
	assert.Equal(t, aws.ToString(sg.GroupId), aws.ToString(out.Reservations[0].Instances[0].SecurityGroups[0].GroupId))
}

func runModifyNetworkInterfaceAttributeGroups(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.41.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.41.1.0/24"),
	})
	require.NoError(t, err)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("modify-eni-attr-sg"),
		Description: aws.String("test sg"),
		VpcId:       vpc.Vpc.VpcId,
	})
	require.NoError(t, err)

	eni, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnet.Subnet.SubnetId,
	})
	require.NoError(t, err)
	eniID := eni.NetworkInterface.NetworkInterfaceId

	_, err = client.ModifyNetworkInterfaceAttribute(t.Context(), &ec2sdk.ModifyNetworkInterfaceAttributeInput{
		NetworkInterfaceId: eniID,
		Groups:             []string{aws.ToString(sg.GroupId)},
	})
	require.NoError(t, err)

	out, err := client.DescribeNetworkInterfaces(t.Context(), &ec2sdk.DescribeNetworkInterfacesInput{
		NetworkInterfaceIds: []string{aws.ToString(eniID)},
	})
	require.NoError(t, err)
	require.Len(t, out.NetworkInterfaces, 1)
	require.Len(t, out.NetworkInterfaces[0].Groups, 1)
	assert.Equal(t, aws.ToString(sg.GroupId), aws.ToString(out.NetworkInterfaces[0].Groups[0].GroupId))
}

func runCreateVpcPeeringConnectionPeerFields(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.42.0.0/16")})
	require.NoError(t, err)

	out, err := client.CreateVpcPeeringConnection(t.Context(), &ec2sdk.CreateVpcPeeringConnectionInput{
		VpcId:       vpc.Vpc.VpcId,
		PeerVpcId:   aws.String("vpc-remote-99998888"),
		PeerOwnerId: aws.String("999988887777"),
		PeerRegion:  aws.String("eu-west-1"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.VpcPeeringConnection.AccepterVpcInfo)
	assert.Equal(t, "999988887777", aws.ToString(out.VpcPeeringConnection.AccepterVpcInfo.OwnerId))
	assert.Equal(t, "eu-west-1", aws.ToString(out.VpcPeeringConnection.AccepterVpcInfo.Region))
}

func runCreateVpnGatewayAmazonSideAsn(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	out, err := client.CreateVpnGateway(t.Context(), &ec2sdk.CreateVpnGatewayInput{
		Type:          types.GatewayTypeIpsec1,
		AmazonSideAsn: aws.Int64(65010),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(65010), aws.ToInt64(out.VpnGateway.AmazonSideAsn))
}
