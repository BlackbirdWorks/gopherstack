package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestAssignPrivateIpAddresses_SurfacesAssignedIPs_RealClient covers
// handleAssignPrivateIPAddresses, which pre-fix returned only
// NetworkInterfaceId and a spurious Return. The real
// AssignPrivateIpAddressesOutput deserializer also matches
// "assignedPrivateIpAddressesSet" (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentAssignPrivateIpAddressesOutput has no case
// for "return" at all) -- so a client relying on the auto-allocated IP
// addresses (the common case: assign by count, not by explicit address) saw
// an empty slice pre-fix regardless of what was actually assigned.
func TestAssignPrivateIpAddresses_SurfacesAssignedIPs_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.60.0.0/16")})
	require.NoError(t, err)
	subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId:     vpc.Vpc.VpcId,
		CidrBlock: aws.String("10.60.1.0/24"),
	})
	require.NoError(t, err)
	eni, err := client.CreateNetworkInterface(t.Context(), &ec2sdk.CreateNetworkInterfaceInput{
		SubnetId: subnet.Subnet.SubnetId,
	})
	require.NoError(t, err)

	out, err := client.AssignPrivateIpAddresses(t.Context(), &ec2sdk.AssignPrivateIpAddressesInput{
		NetworkInterfaceId:             eni.NetworkInterface.NetworkInterfaceId,
		SecondaryPrivateIpAddressCount: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(
		t, out.AssignedPrivateIpAddresses, 2,
		"pre-fix this field was never rendered, only NetworkInterfaceId and a bare Return bool",
	)
	assert.NotEmpty(t, aws.ToString(out.AssignedPrivateIpAddresses[0].PrivateIpAddress))
}

// TestAssignPrivateNatGatewayAddress_SurfacesAddresses_RealClient covers
// handleAssignPrivateNatGatewayAddress, which pre-fix rendered a bare
// <return>true</return> via stubResponse. The real
// AssignPrivateNatGatewayAddressOutput has no Return member -- only
// natGatewayAddressSet and natGatewayId (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentAssignPrivateNatGatewayAddressOutput has
// no case for "return") -- the same shape the sibling Associate/Disassociate/
// UnassignPrivateNatGatewayAddress ops already render correctly.
func TestAssignPrivateNatGatewayAddress_SurfacesAddresses_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	alloc, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	nat, err := client.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		SubnetId:     aws.String("subnet-default"),
		AllocationId: alloc.AllocationId,
	})
	require.NoError(t, err)

	out, err := client.AssignPrivateNatGatewayAddress(t.Context(), &ec2sdk.AssignPrivateNatGatewayAddressInput{
		NatGatewayId:          nat.NatGateway.NatGatewayId,
		PrivateIpAddressCount: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(
		t, aws.ToString(nat.NatGateway.NatGatewayId), aws.ToString(out.NatGatewayId),
		"NatGatewayId empty - never rendered pre-fix",
	)
	require.NotEmpty(t, out.NatGatewayAddresses, "NatGatewayAddresses empty - never rendered pre-fix")
}
