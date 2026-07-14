package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParityFinal_UnassignPrivateNatGatewayAddress(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	nat, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)

	require.NoError(t, b.AssignPrivateNatGatewayAddress(nat.ID))
	require.NoError(t, b.AssignPrivateNatGatewayAddress(nat.ID))

	described := b.DescribeNatGateways([]string{nat.ID})
	require.Len(t, described, 1)
	require.Len(t, described[0].SecondaryPrivateIPs, 2)

	firstSecondary := described[0].SecondaryPrivateIPs[0]

	updated, err := b.UnassignPrivateNatGatewayAddress(nat.ID, []string{firstSecondary})
	require.NoError(t, err)
	assert.Len(t, updated.SecondaryPrivateIPs, 1)
	assert.NotContains(t, updated.SecondaryPrivateIPs, firstSecondary)

	_, err = b.UnassignPrivateNatGatewayAddress("nat-missing", []string{firstSecondary})
	require.ErrorIs(t, err, ec2.ErrNatGatewayNotFound)

	_, err = b.UnassignPrivateNatGatewayAddress(nat.ID, nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

// ---- Image extras ----
