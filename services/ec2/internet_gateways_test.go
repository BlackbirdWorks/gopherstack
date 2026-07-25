package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestInternetGatewayOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{name: "create", op: "create", wantErr: false},
		{name: "describe_all", op: "describe_all", wantErr: false},
		{name: "delete", op: "delete", wantErr: false},
		{name: "delete_nonexistent", op: "delete_nonexistent", wantErr: true},
		{name: "attach_detach", op: "attach_detach", wantErr: false},
		{name: "attach_bad_igw", op: "attach_bad_igw", wantErr: true},
		{name: "attach_bad_vpc", op: "attach_bad_vpc", wantErr: true},
		{name: "detach_not_attached", op: "detach_not_attached", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			switch tt.op {
			case "create":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				assert.NotEmpty(t, igw.ID)
				assert.Empty(t, igw.Attachments)

			case "describe_all":
				_, err := b.CreateInternetGateway()
				require.NoError(t, err)
				igws := b.DescribeInternetGateways(nil)
				assert.NotEmpty(t, igws)

			case "delete":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				err = b.DeleteInternetGateway(igw.ID)
				require.NoError(t, err)
				igws := b.DescribeInternetGateways([]string{igw.ID})
				assert.Empty(t, igws)

			case "delete_nonexistent":
				err := b.DeleteInternetGateway("igw-nonexistent")
				require.Error(t, err)

			case "attach_detach":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				vpc, err := b.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)
				err = b.AttachInternetGateway(igw.ID, vpc.ID)
				require.NoError(t, err)
				igws := b.DescribeInternetGateways([]string{igw.ID})
				require.Len(t, igws, 1)
				assert.Len(t, igws[0].Attachments, 1)
				err = b.DetachInternetGateway(igw.ID, vpc.ID)
				require.NoError(t, err)
				igws = b.DescribeInternetGateways([]string{igw.ID})
				require.Len(t, igws, 1)
				assert.Empty(t, igws[0].Attachments)

			case "attach_bad_igw":
				vpc, err := b.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)
				err = b.AttachInternetGateway("igw-nonexistent", vpc.ID)
				require.Error(t, err)

			case "attach_bad_vpc":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				err = b.AttachInternetGateway(igw.ID, "vpc-nonexistent")
				require.Error(t, err)

			case "detach_not_attached":
				igw, err := b.CreateInternetGateway()
				require.NoError(t, err)
				err = b.DetachInternetGateway(igw.ID, "vpc-default")
				require.Error(t, err)
			}
		})
	}
}

// TestAttachInternetGateway_AlreadyAssociated verifies that AttachInternetGateway
// matches real AWS: an IGW can only be attached to one VPC at a time, and a VPC
// can only have one IGW attached at a time. Either conflict returns
// Resource.AlreadyAssociated rather than silently allowing multiple attachments.
func TestAttachInternetGateway_AlreadyAssociated(t *testing.T) {
	t.Parallel()

	t.Run("igw already attached elsewhere", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		igw, err := b.CreateInternetGateway()
		require.NoError(t, err)
		vpc1, err := b.CreateVpc("10.1.0.0/16")
		require.NoError(t, err)
		vpc2, err := b.CreateVpc("10.2.0.0/16")
		require.NoError(t, err)

		require.NoError(t, b.AttachInternetGateway(igw.ID, vpc1.ID))

		err = b.AttachInternetGateway(igw.ID, vpc2.ID)
		require.Error(t, err)
		assert.ErrorIs(t, err, ec2.ErrResourceAlreadyAssociated)
	})

	t.Run("vpc already has a different igw attached", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()

		igw1, err := b.CreateInternetGateway()
		require.NoError(t, err)
		igw2, err := b.CreateInternetGateway()
		require.NoError(t, err)
		vpc, err := b.CreateVpc("10.3.0.0/16")
		require.NoError(t, err)

		require.NoError(t, b.AttachInternetGateway(igw1.ID, vpc.ID))

		err = b.AttachInternetGateway(igw2.ID, vpc.ID)
		require.Error(t, err)
		assert.ErrorIs(t, err, ec2.ErrResourceAlreadyAssociated)
	})
}

// TestDeleteInternetGateway_DependencyViolation verifies that DeleteInternetGateway
// fails with DependencyViolation while the IGW is still attached to a VPC, and
// succeeds once it has been detached — matching real AWS (no implicit detach).
func TestDeleteInternetGateway_DependencyViolation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	igw, err := b.CreateInternetGateway()
	require.NoError(t, err)
	vpc, err := b.CreateVpc("10.4.0.0/16")
	require.NoError(t, err)

	require.NoError(t, b.AttachInternetGateway(igw.ID, vpc.ID))

	err = b.DeleteInternetGateway(igw.ID)
	require.Error(t, err, "DeleteInternetGateway must fail while attached")
	require.ErrorIs(t, err, ec2.ErrDependencyViolation)
	assert.NotEmpty(t, b.DescribeInternetGateways([]string{igw.ID}))

	require.NoError(t, b.DetachInternetGateway(igw.ID, vpc.ID))
	require.NoError(t, b.DeleteInternetGateway(igw.ID))
	assert.Empty(t, b.DescribeInternetGateways([]string{igw.ID}))
}
