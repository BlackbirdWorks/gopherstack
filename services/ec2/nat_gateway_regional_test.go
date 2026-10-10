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

func TestRealClient_RegionalNatGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *ec2sdk.Client)
		name string
	}{
		{runRegionalNatAuto, "auto_expand_retract"},
		{runRegionalNatExplicit, "explicit_zones"},
		{runRegionalNatInvalid, "invalid_requests"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			tt.run(t, newTestEC2Client(t, h))
		})
	}
}

func natTestVPC(t *testing.T, client *ec2sdk.Client) string {
	t.Helper()

	out, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)

	return aws.ToString(out.Vpc.VpcId)
}

func natTestSubnet(t *testing.T, client *ec2sdk.Client, vpcID, cidr, az string) string {
	t.Helper()

	out, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String(cidr), AvailabilityZone: aws.String(az),
	})
	require.NoError(t, err)

	return aws.ToString(out.Subnet.SubnetId)
}

func describeNat(t *testing.T, client *ec2sdk.Client, id string) types.NatGateway {
	t.Helper()

	out, err := client.DescribeNatGateways(t.Context(), &ec2sdk.DescribeNatGatewaysInput{NatGatewayIds: []string{id}})
	require.NoError(t, err)
	require.Len(t, out.NatGateways, 1)

	return out.NatGateways[0]
}

func runRegionalNatAuto(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcID := natTestVPC(t, client)
	natTestSubnet(t, client, vpcID, "10.0.1.0/24", "us-east-1a")
	subB := natTestSubnet(t, client, vpcID, "10.0.2.0/24", "us-east-1b")

	out, err := client.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		AvailabilityMode: types.AvailabilityModeRegional, VpcId: aws.String(vpcID),
	})
	require.NoError(t, err)

	id := aws.ToString(out.NatGateway.NatGatewayId)
	assert.Equal(t, types.AvailabilityModeRegional, out.NatGateway.AvailabilityMode)
	assert.Equal(t, vpcID, aws.ToString(out.NatGateway.VpcId))
	assert.Equal(t, types.AutoProvisionZonesStateEnabled, out.NatGateway.AutoProvisionZones)
	assert.Equal(t, types.AutoScalingIpsStateEnabled, out.NatGateway.AutoScalingIps)
	assert.Nil(t, out.NatGateway.SubnetId)
	require.Len(t, out.NatGateway.NatGatewayAddresses, 2)
	assert.Equal(t, "us-east-1a", aws.ToString(out.NatGateway.NatGatewayAddresses[0].AvailabilityZone))
	assert.Equal(t, "use1-az1", aws.ToString(out.NatGateway.NatGatewayAddresses[0].AvailabilityZoneId))

	natTestSubnet(t, client, vpcID, "10.0.3.0/24", "us-east-1c")
	assert.Len(t, describeNat(t, client, id).NatGatewayAddresses, 3)

	_, err = client.DeleteSubnet(t.Context(), &ec2sdk.DeleteSubnetInput{SubnetId: aws.String(subB)})
	require.NoError(t, err)

	got := describeNat(t, client, id)
	require.Len(t, got.NatGatewayAddresses, 2)

	for _, a := range got.NatGatewayAddresses {
		assert.NotEqual(t, "us-east-1b", aws.ToString(a.AvailabilityZone))
	}

	_, err = client.DeleteNatGateway(t.Context(), &ec2sdk.DeleteNatGatewayInput{NatGatewayId: aws.String(id)})
	require.NoError(t, err)
}

func runRegionalNatExplicit(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcID := natTestVPC(t, client)
	natTestSubnet(t, client, vpcID, "10.0.1.0/24", "us-east-1a")

	eip1, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	eip2, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	out, err := client.CreateNatGateway(t.Context(), &ec2sdk.CreateNatGatewayInput{
		AvailabilityMode: types.AvailabilityModeRegional,
		VpcId:            aws.String(vpcID),
		AvailabilityZoneAddresses: []types.AvailabilityZoneAddress{
			{AvailabilityZone: aws.String("us-east-1a"), AllocationIds: []string{aws.ToString(eip1.AllocationId)}},
		},
	})
	require.NoError(t, err)

	id := aws.ToString(out.NatGateway.NatGatewayId)
	assert.Equal(t, types.AutoProvisionZonesStateDisabled, out.NatGateway.AutoProvisionZones)
	require.Len(t, out.NatGateway.NatGatewayAddresses, 1)
	assert.Equal(t, aws.ToString(eip1.AllocationId), aws.ToString(out.NatGateway.NatGatewayAddresses[0].AllocationId))

	natTestSubnet(t, client, vpcID, "10.0.2.0/24", "us-east-1b")
	assert.Len(t, describeNat(t, client, id).NatGatewayAddresses, 1)

	assoc, err := client.AssociateNatGatewayAddress(t.Context(), &ec2sdk.AssociateNatGatewayAddressInput{
		NatGatewayId:       aws.String(id),
		AvailabilityZoneId: aws.String("use1-az2"),
		AllocationIds:      []string{aws.ToString(eip2.AllocationId)},
	})
	require.NoError(t, err)
	require.Len(t, assoc.NatGatewayAddresses, 2)

	var assocID string

	for _, a := range assoc.NatGatewayAddresses {
		if aws.ToString(a.AllocationId) == aws.ToString(eip2.AllocationId) {
			assert.Equal(t, "us-east-1b", aws.ToString(a.AvailabilityZone))
			assocID = aws.ToString(a.AssociationId)
		}
	}

	require.NotEmpty(t, assocID)

	_, err = client.DisassociateNatGatewayAddress(t.Context(), &ec2sdk.DisassociateNatGatewayAddressInput{
		NatGatewayId: aws.String(id), AssociationIds: []string{assocID},
	})
	require.NoError(t, err)
	assert.Len(t, describeNat(t, client, id).NatGatewayAddresses, 1)
}

func runRegionalNatInvalid(t *testing.T, client *ec2sdk.Client) {
	t.Helper()

	vpcID := natTestVPC(t, client)
	eip, err := client.AllocateAddress(t.Context(), &ec2sdk.AllocateAddressInput{})
	require.NoError(t, err)

	bad := []ec2sdk.CreateNatGatewayInput{
		{AvailabilityMode: types.AvailabilityModeRegional},
		{AvailabilityMode: types.AvailabilityModeRegional, VpcId: aws.String("vpc-nope")},
		{AvailabilityMode: types.AvailabilityModeRegional, VpcId: aws.String(vpcID), SubnetId: aws.String("subnet-1")},
		{
			AvailabilityMode: types.AvailabilityModeRegional, VpcId: aws.String(vpcID),
			AvailabilityZoneAddresses: []types.AvailabilityZoneAddress{
				{AvailabilityZone: aws.String("eu-west-1a"), AllocationIds: []string{aws.ToString(eip.AllocationId)}},
			},
		},
		{
			AvailabilityMode: types.AvailabilityModeRegional, VpcId: aws.String(vpcID),
			AvailabilityZoneAddresses: []types.AvailabilityZoneAddress{
				{AvailabilityZone: aws.String("us-east-1a"), AllocationIds: []string{"eipalloc-nope"}},
			},
		},
		{AvailabilityMode: types.AvailabilityModeZonal, VpcId: aws.String(vpcID)},
	}

	for _, in := range bad {
		_, err = client.CreateNatGateway(t.Context(), &in)
		require.Error(t, err)
	}
}
