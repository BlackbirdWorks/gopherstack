package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnassignPrivateNatGatewayAddress(t *testing.T) {
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

func TestNatGatewayOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create", wantErr: false},
		{name: "create_bad_subnet", op: "create_bad_subnet", wantErr: true},
		{name: "create_bad_alloc", op: "create_bad_alloc", wantErr: true},
		{name: "describe_all", op: "describe_all", wantErr: false},
		{name: "delete", op: "delete", wantErr: false},
		{name: "delete_nonexistent", op: "delete_nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
				require.NoError(t, err)
				assert.NotEmpty(t, ngw.ID)
				assert.Equal(t, "available", ngw.State)
				assert.NotEmpty(t, ngw.PublicIP)
				assert.NotEmpty(t, ngw.PrivateIP)

			case "create_bad_subnet":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				_, err = b.CreateNatGateway("subnet-nonexistent", addr.AllocationID)
				require.Error(t, err)

			case "create_bad_alloc":
				_, err := b.CreateNatGateway("subnet-default", "eipalloc-nonexistent")
				require.Error(t, err)

			case "describe_all":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				_, err = b.CreateNatGateway("subnet-default", addr.AllocationID)
				require.NoError(t, err)
				ngws := b.DescribeNatGateways(nil)
				assert.NotEmpty(t, ngws)

			case "delete":
				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
				require.NoError(t, err)
				err = b.DeleteNatGateway(ngw.ID)
				require.NoError(t, err)
				ngws := b.DescribeNatGateways([]string{ngw.ID})
				assert.Empty(t, ngws)

			case "delete_nonexistent":
				err := b.DeleteNatGateway("nat-nonexistent")
				require.Error(t, err)
			}
		})
	}
}
