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

// TestRealClient_NetworkAndRouteFields covers the gopherstack-xhu2t/99nj
// ec2query filter/field sweep's 2026-09-13 long-tail fixes:
// AttachNetworkInterface.NetworkCardIndex (a real dropped-parameter bug --
// the response always hardcoded 0 regardless of what was requested),
// CreateNetworkInterface.InterfaceType, and ReplaceRoute.LocalTarget. Each
// row gets its own fresh handler+backend so rows can run in parallel
// without shared state.
func TestRealClient_NetworkAndRouteFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *ec2sdk.Client)
		name string
	}{
		{runAttachNetworkInterfaceCardIndex, "attach_network_interface_card_index"},
		{runCreateNetworkInterfaceType, "create_network_interface_type"},
		{runReplaceRouteLocalTarget, "replace_route_local_target"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			h.AccountID = "000000000000"
			client := newTestEC2Client(t, h)
			tt.run(t, client)
		})
	}
}

// runAttachNetworkInterfaceCardIndex covers AttachNetworkInterface's
// NetworkCardIndex: previously the response always hardcoded 0 regardless
// of what was requested.
func runAttachNetworkInterfaceCardIndex(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: vpcOut.Vpc.VpcId, CidrBlock: aws.String("10.0.0.0/24"),
	})
	require.NoError(t, err)
	eniOut, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnetOut.Subnet.SubnetId,
	})
	require.NoError(t, err)

	runOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-parity-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	attOut, err := client.AttachNetworkInterface(t.Context(), &ec2sdk.AttachNetworkInterfaceInput{
		NetworkInterfaceId: eniOut.NetworkInterface.NetworkInterfaceId,
		InstanceId:         aws.String(instanceID),
		DeviceIndex:        aws.Int32(1),
		NetworkCardIndex:   aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(attOut.NetworkCardIndex))
}

// runCreateNetworkInterfaceType covers CreateNetworkInterface's
// InterfaceType (default "interface" when omitted, echoed on
// DescribeNetworkInterfaces).
func runCreateNetworkInterfaceType(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	subnetOut, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: vpcOut.Vpc.VpcId, CidrBlock: aws.String("10.0.0.0/24"),
	})
	require.NoError(t, err)

	efaOut, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnetOut.Subnet.SubnetId, InterfaceType: types.NetworkInterfaceCreationTypeEfa,
	})
	require.NoError(t, err)
	assert.Equal(t, types.NetworkInterfaceTypeEfa, efaOut.NetworkInterface.InterfaceType)

	defaultOut, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnetOut.Subnet.SubnetId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.NetworkInterfaceTypeInterface, defaultOut.NetworkInterface.InterfaceType)
}

// runReplaceRouteLocalTarget covers ReplaceRoute's LocalTarget: when true,
// it overrides GatewayId/NatGatewayId and resets the route to the implicit
// "local" target.
func runReplaceRouteLocalTarget(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcOut, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	igwOut, err := client.CreateInternetGateway(t.Context(), &ec2sdk.CreateInternetGatewayInput{})
	require.NoError(t, err)
	igwID := aws.ToString(igwOut.InternetGateway.InternetGatewayId)
	_, err = client.AttachInternetGateway(t.Context(), &ec2sdk.AttachInternetGatewayInput{
		InternetGatewayId: aws.String(igwID), VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	rtOut, err := client.CreateRouteTable(t.Context(), &ec2sdk.CreateRouteTableInput{VpcId: aws.String(vpcID)})
	require.NoError(t, err)
	rtID := aws.ToString(rtOut.RouteTable.RouteTableId)

	_, err = client.CreateRoute(t.Context(), &ec2sdk.CreateRouteInput{
		RouteTableId: aws.String(rtID), DestinationCidrBlock: aws.String("172.16.0.0/16"),
		GatewayId: aws.String(igwID),
	})
	require.NoError(t, err)

	_, err = client.ReplaceRoute(t.Context(), &ec2sdk.ReplaceRouteInput{
		RouteTableId: aws.String(rtID), DestinationCidrBlock: aws.String("172.16.0.0/16"),
		LocalTarget: aws.Bool(true),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeRouteTables(t.Context(), &ec2sdk.DescribeRouteTablesInput{
		RouteTableIds: []string{rtID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.RouteTables, 1)

	var found bool
	for _, r := range descOut.RouteTables[0].Routes {
		if aws.ToString(r.DestinationCidrBlock) == "172.16.0.0/16" {
			found = true
			assert.Equal(t, "local", aws.ToString(r.GatewayId), "LocalTarget=true must reset the route to local")
		}
	}
	assert.True(t, found, "replaced route must still be present")
}
