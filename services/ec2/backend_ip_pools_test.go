package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestBackend_CoipPool_CreateCidrDescribeUsageRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	pool, err := b.CreateCoipPool("lgw-rtb-12345", map[string]string{"Name": "my-coip"})
	require.NoError(t, err)
	assert.Contains(t, pool.PoolID, "ipv4pool-coip-")
	assert.Equal(t, "lgw-rtb-12345", pool.LocalGatewayRouteTableID)

	pools := b.DescribeCoipPools([]string{pool.PoolID})
	require.Len(t, pools, 1)
	assert.Equal(t, pool.PoolID, pools[0].PoolID)

	cidr, err := b.CreateCoipCidr(pool.PoolID, "10.0.0.0/24")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.0/24", cidr.Cidr)

	pools = b.DescribeCoipPools([]string{pool.PoolID})
	require.Len(t, pools, 1)
	assert.Contains(t, pools[0].PoolCidrs, "10.0.0.0/24")

	usage, err := b.GetCoipPoolUsage(pool.PoolID)
	require.NoError(t, err)
	assert.Equal(t, "lgw-rtb-12345", usage.LocalGatewayRouteTableID)

	_, err = b.DeleteCoipCidr(pool.PoolID, "10.0.0.0/24")
	require.NoError(t, err)

	pools = b.DescribeCoipPools([]string{pool.PoolID})
	require.Len(t, pools, 1)
	assert.Empty(t, pools[0].PoolCidrs)
}

func TestBackend_CoipPool_DescribeUnknownIDReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Empty(t, b.DescribeCoipPools([]string{"ipv4pool-coip-doesnotexist"}))
}

func TestBackend_CoipPool_DeleteCidrNotFoundFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	pool, err := b.CreateCoipPool("lgw-rtb-12345", nil)
	require.NoError(t, err)

	_, err = b.DeleteCoipCidr(pool.PoolID, "192.0.2.0/24")
	require.Error(t, err)
}

func TestBackend_CoipPool_CreateWithoutRouteTableIDFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateCoipPool("", nil)
	require.Error(t, err)
}

func TestBackend_CoipPool_DeleteRemovesPoolAndCidrs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	pool, err := b.CreateCoipPool("lgw-rtb-99999", nil)
	require.NoError(t, err)

	_, err = b.CreateCoipCidr(pool.PoolID, "192.0.2.0/24")
	require.NoError(t, err)

	_, err = b.DeleteCoipPool(pool.PoolID)
	require.NoError(t, err)
	assert.Empty(t, b.DescribeCoipPools([]string{pool.PoolID}))

	_, err = b.GetCoipPoolUsage(pool.PoolID)
	require.Error(t, err)
}

func TestBackend_CoipPool_DeleteUnknownFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.DeleteCoipPool("ipv4pool-coip-doesnotexist")
	require.Error(t, err)
}

func TestBackend_PublicIpv4Pool_CreateDescribeRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	pool := b.CreatePublicIpv4Pool("", map[string]string{"Name": "my-pool"})
	assert.Contains(t, pool.PoolID, "ipv4pool-ec2-")
	assert.Equal(t, "us-east-1", pool.NetworkBorderGroup)

	pools := b.DescribePublicIpv4Pools([]string{pool.PoolID})
	require.Len(t, pools, 1)
	assert.Equal(t, pool.PoolID, pools[0].PoolID)
}

func TestBackend_PublicIpv4Pool_DescribeUnknownIDReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Empty(t, b.DescribePublicIpv4Pools([]string{"ipv4pool-ec2-doesnotexist"}))
}

func TestBackend_PublicIpv4Pool_ProvisionDeprovisionCidrRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	pool := b.CreatePublicIpv4Pool("", nil)

	r, err := b.ProvisionPublicIpv4PoolCidr(pool.PoolID, 24)
	require.NoError(t, err)
	require.NotEmpty(t, r.FirstAddress)
	assert.Equal(t, int32(256), r.AddressCount)

	pools := b.DescribePublicIpv4Pools([]string{pool.PoolID})
	require.Len(t, pools, 1)
	require.Len(t, pools[0].PoolAddressRanges, 1)

	err = b.DeprovisionPublicIpv4PoolCidr(pool.PoolID, r.FirstAddress)
	require.NoError(t, err)

	pools = b.DescribePublicIpv4Pools([]string{pool.PoolID})
	require.Len(t, pools, 1)
	assert.Empty(t, pools[0].PoolAddressRanges)
}

func TestBackend_PublicIpv4Pool_ProvisionUnknownPoolFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.ProvisionPublicIpv4PoolCidr("ipv4pool-ec2-doesnotexist", 24)
	require.Error(t, err)
}

func TestBackend_PublicIpv4Pool_DeprovisionUnknownCidrFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	pool := b.CreatePublicIpv4Pool("", nil)

	err := b.DeprovisionPublicIpv4PoolCidr(pool.PoolID, "198.51.200.0")
	require.Error(t, err)
}

func TestBackend_PublicIpv4Pool_DeleteThenDescribeReturnsNothing(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	pool := b.CreatePublicIpv4Pool("us-east-1a", nil)

	require.NoError(t, b.DeletePublicIpv4Pool(pool.PoolID))
	assert.Empty(t, b.DescribePublicIpv4Pools([]string{pool.PoolID}))
}

func TestBackend_PublicIpv4Pool_DeleteUnknownFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.DeletePublicIpv4Pool("ipv4pool-ec2-doesnotexist")
	require.Error(t, err)
}

func TestBackend_Ipv6Pool_DescribeAndAssociatedCidrsRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	pool := b.CreateIpv6Pool("my ipv6 pool", []string{"2001:db8::/32"})
	assert.Contains(t, pool.PoolID, "ipv6pool-ec2-")

	pools := b.DescribeIpv6Pools([]string{pool.PoolID})
	require.Len(t, pools, 1)
	assert.Equal(t, "my ipv6 pool", pools[0].Description)

	assocs, err := b.GetAssociatedIpv6PoolCidrs(pool.PoolID)
	require.NoError(t, err)
	require.Len(t, assocs, 1)
	assert.Equal(t, "2001:db8::/32", assocs[0].Ipv6Cidr)
}

func TestBackend_Ipv6Pool_DescribeUnknownIDReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Empty(t, b.DescribeIpv6Pools([]string{"ipv6pool-ec2-doesnotexist"}))
}

func TestBackend_Ipv6Pool_GetAssociatedCidrsUnknownPoolFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.GetAssociatedIpv6PoolCidrs("ipv6pool-ec2-doesnotexist")
	require.Error(t, err)
}
