package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublicIP_SubnetMapPublicIpOnLaunch(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.1.1.0/24", "us-east-1a")
	require.NoError(t, err)

	// Before enabling MapPublicIpOnLaunch, instance should have no public IP.
	instances, err := b.RunInstances("ami-123", "t3.micro", subnet.ID, 1)
	require.NoError(t, err)
	assert.Empty(
		t,
		instances[0].PublicIPAddress,
		"no public IP before enabling MapPublicIpOnLaunch",
	)

	// Enable MapPublicIpOnLaunch.
	err = b.ModifySubnetAttribute(subnet.ID, "mapPublicIpOnLaunch", true)
	require.NoError(t, err)

	// Now instance should get a public IP.
	instances2, err := b.RunInstances("ami-123", "t3.micro", subnet.ID, 1)
	require.NoError(t, err)
	assert.NotEmpty(
		t,
		instances2[0].PublicIPAddress,
		"public IP should be assigned when MapPublicIpOnLaunch=true",
	)
	assert.NotEmpty(
		t,
		instances2[0].PublicDNSName,
		"public DNS should be assigned when MapPublicIpOnLaunch=true",
	)
}

func TestModifySubnetAttribute_PersistsValue(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.2.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.2.1.0/24", "us-east-1a")
	require.NoError(t, err)
	assert.False(t, subnet.MapPublicIPOnLaunch)

	err = b.ModifySubnetAttribute(subnet.ID, "mapPublicIpOnLaunch", true)
	require.NoError(t, err)

	subnets := b.DescribeSubnets([]string{subnet.ID})
	require.Len(t, subnets, 1)
	assert.True(t, subnets[0].MapPublicIPOnLaunch)
}

// ---- Gap 3: Pagination ----

// TestModifySubnetAttribute verifies subnet attribute modification.
func TestModifySubnetAttribute(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	require.NoError(t, b.ModifySubnetAttribute("subnet-default", "mapPublicIpOnLaunch", true))
	require.Error(t, b.ModifySubnetAttribute("subnet-nonexistent", "mapPublicIpOnLaunch", true))
	require.Error(t, b.ModifySubnetAttribute("subnet-default", "unknownAttr", true))
}

// TestCreateNetworkAcl verifies NACL creation.

// TestDescribeSubnetsByVPC verifies indexed VPC-based subnet lookup.
func TestDescribeSubnetsByVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	subs := b.DescribeSubnetsByVPC("vpc-default")
	require.NotEmpty(t, subs)

	for _, s := range subs {
		assert.Equal(t, "vpc-default", s.VPCID)
	}
}

// TestDescribeInstancesByVPC verifies secondary-index instance lookup.

func TestCreateSubnet_CIDRConflict(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.3.0.0/16")
	require.NoError(t, err)

	_, err = b.CreateSubnet(vpc.ID, "10.3.1.0/24", "us-east-1a")
	require.NoError(t, err)

	// Same CIDR in same VPC should conflict.
	_, err = b.CreateSubnet(vpc.ID, "10.3.1.0/24", "us-east-1b")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrCIDRConflict)
}

func TestCreateSubnet_NotInVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	vpc, err := b.CreateVpc("10.4.0.0/16")
	require.NoError(t, err)

	// Subnet outside VPC CIDR should fail.
	_, err = b.CreateSubnet(vpc.ID, "192.168.1.0/24", "us-east-1a")
	require.Error(t, err)
	assert.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

// ---- Optimization: spotFleetHistory cap ----
