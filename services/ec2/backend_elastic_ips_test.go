package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParityFinal_MoveAddressToVpcAndDescribeMovingAddresses(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	moved, err := b.MoveAddressToVpc(addr.PublicIP)
	require.NoError(t, err)
	assert.Equal(t, addr.AllocationID, moved.AllocationID)

	statuses := b.DescribeMovingAddresses(nil)
	require.Len(t, statuses, 1)
	assert.Equal(t, addr.PublicIP, statuses[0].PublicIP)
	assert.Equal(t, "movingToVpc", statuses[0].MoveStatus)

	filtered := b.DescribeMovingAddresses([]string{addr.PublicIP})
	require.Len(t, filtered, 1)

	none := b.DescribeMovingAddresses([]string{"1.2.3.4"})
	assert.Empty(t, none)

	_, err = b.MoveAddressToVpc("9.9.9.9")
	require.ErrorIs(t, err, ec2.ErrPublicIPNotFound)
}
