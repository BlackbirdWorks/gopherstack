package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestBackend_SecondaryNetwork_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	net, err := b.CreateSecondaryNetwork("10.100.0.0/16", "rdma", nil)
	require.NoError(t, err)
	assert.Contains(t, net.SecondaryNetworkID, "secnet-")
	assert.Equal(t, "rdma", net.Type)
	assert.Equal(t, "create-complete", net.State)
	require.Len(t, net.Ipv4CidrBlockAssociations, 1)
	assert.Equal(t, "10.100.0.0/16", net.Ipv4CidrBlockAssociations[0].CidrBlock)

	found := b.DescribeSecondaryNetworks([]string{net.SecondaryNetworkID})
	require.Len(t, found, 1)
	assert.Equal(t, net.SecondaryNetworkID, found[0].SecondaryNetworkID)

	deleted, err := b.DeleteSecondaryNetwork(net.SecondaryNetworkID)
	require.NoError(t, err)
	assert.Equal(t, "delete-complete", deleted.State)

	assert.Empty(t, b.DescribeSecondaryNetworks([]string{net.SecondaryNetworkID}))
}

func TestBackend_SecondaryNetwork_RequiresCidrBlock(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateSecondaryNetwork("", "rdma", nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_DeleteSecondaryNetwork_NotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.DeleteSecondaryNetwork("secnet-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrSecondaryNetworkNotFound)
}

func TestBackend_DeleteSecondaryNetwork_DependencyViolationWithSubnets(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	net, err := b.CreateSecondaryNetwork("10.100.0.0/16", "rdma", nil)
	require.NoError(t, err)

	_, err = b.CreateSecondarySubnet("10.100.1.0/24", net.SecondaryNetworkID, "", "", nil)
	require.NoError(t, err)

	_, err = b.DeleteSecondaryNetwork(net.SecondaryNetworkID)
	require.ErrorIs(t, err, ec2.ErrSecondaryNetworkHasSubnets)
}

func TestBackend_SecondarySubnet_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	net, err := b.CreateSecondaryNetwork("10.100.0.0/16", "rdma", nil)
	require.NoError(t, err)

	sub, err := b.CreateSecondarySubnet("10.100.1.0/24", net.SecondaryNetworkID, "us-east-1a", "", nil)
	require.NoError(t, err)
	assert.Contains(t, sub.SecondarySubnetID, "secsubnet-")
	assert.Equal(t, "us-east-1a", sub.AvailabilityZone)
	assert.Equal(t, "rdma", sub.SecondaryNetworkType)
	assert.Equal(t, "create-complete", sub.State)

	found := b.DescribeSecondarySubnets([]string{sub.SecondarySubnetID})
	require.Len(t, found, 1)

	deleted, err := b.DeleteSecondarySubnet(sub.SecondarySubnetID)
	require.NoError(t, err)
	assert.Equal(t, "delete-complete", deleted.State)
	assert.Empty(t, b.DescribeSecondarySubnets([]string{sub.SecondarySubnetID}))
}

func TestBackend_SecondarySubnet_UnknownNetworkFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateSecondarySubnet("10.100.1.0/24", "secnet-doesnotexist", "", "", nil)
	require.ErrorIs(t, err, ec2.ErrSecondaryNetworkNotFound)
}

func TestBackend_DeleteSecondarySubnet_NotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.DeleteSecondarySubnet("secsubnet-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrSecondarySubnetNotFound)
}

func TestBackend_SecondaryInterface_SeedAndDescribe(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	assert.Empty(t, b.DescribeSecondaryInterfaces(nil))

	si, err := b.SeedSecondaryInterface(ec2.SecondaryInterface{
		SecondaryNetworkID: "secnet-abc", Status: "available",
	})
	require.NoError(t, err)
	assert.Contains(t, si.SecondaryInterfaceID, "secni-")
	assert.Equal(t, "123456789012", si.OwnerID)

	found := b.DescribeSecondaryInterfaces([]string{si.SecondaryInterfaceID})
	require.Len(t, found, 1)
	assert.Equal(t, "secnet-abc", found[0].SecondaryNetworkID)
}

func TestBackend_OutpostLag_SeedAndDescribe(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	assert.Empty(t, b.DescribeOutpostLags(nil))

	lag, err := b.SeedOutpostLag(ec2.OutpostLag{OutpostArn: "arn:aws:outposts:us-east-1:123456789012:outpost/op-1"})
	require.NoError(t, err)
	assert.Contains(t, lag.OutpostLagID, "lag-")
	assert.Equal(t, "available", lag.State)

	found := b.DescribeOutpostLags([]string{lag.OutpostLagID})
	require.Len(t, found, 1)
}

func TestBackend_ServiceLinkVirtualInterface_SeedAndDescribe(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	assert.Empty(t, b.DescribeServiceLinkVirtualInterfaces(nil))

	vif, err := b.SeedServiceLinkVirtualInterface(ec2.ServiceLinkVirtualInterface{
		OutpostID: "op-1", LocalAddress: "10.0.0.1",
	})
	require.NoError(t, err)
	assert.Contains(t, vif.ServiceLinkVirtualInterfaceID, "svif-")
	assert.Equal(t, "available", vif.ConfigurationState)

	found := b.DescribeServiceLinkVirtualInterfaces([]string{vif.ServiceLinkVirtualInterfaceID})
	require.Len(t, found, 1)
	assert.Equal(t, "10.0.0.1", found[0].LocalAddress)
}
