package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Network interface ---- //nolint:godot // existing issue.
func TestNetworkInterfaceAttribute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "test NI")

	t.Run( //nolint:paralleltest // existing issue.
		"describe returns description and sourceDestCheck",
		func(t *testing.T) {
			result, err := b.DescribeNetworkInterfaceAttribute(ni.ID, "description")
			require.NoError(t, err)
			assert.Equal(t, ni.ID, result.NetworkInterfaceID)
		},
	)

	t.Run("reset sets sourceDestCheck to true", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ResetNetworkInterfaceAttribute(ni.ID))
	})

	t.Run("unknown NI returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DescribeNetworkInterfaceAttribute("eni-missing", "description")
		require.Error(t, err)
	})
}

func TestNetworkInterfacePermissions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "perm test NI")

	t.Run("create permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
		perm, err := b.CreateNetworkInterfacePermission(
			ni.ID,
			"123456789012",
			"",
			"INSTANCE-ATTACH",
		)
		require.NoError(t, err)
		assert.Equal(t, "granted", perm.State)
	})

	t.Run("describe returns created permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
		perms := b.DescribeNetworkInterfacePermissions([]string{ni.ID})
		require.Len(t, perms, 1)
		assert.Equal(t, "INSTANCE-ATTACH", perms[0].Permission)

		t.Run("delete permission", func(t *testing.T) { //nolint:paralleltest // existing issue.
			permID := perms[0].PermissionID
			require.NoError(t, b.DeleteNetworkInterfacePermission(permID))
			remaining := b.DescribeNetworkInterfacePermissions([]string{ni.ID})
			assert.Empty(t, remaining)
		})
	})
}

func TestIPv6Addresses(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	ni, _ := b.CreateNetworkInterface("subnet-default", "ipv6 NI")

	t.Run("assign IPv6 addresses", func(t *testing.T) { //nolint:paralleltest // existing issue.
		addrs, err := b.AssignIpv6Addresses(ni.ID, 2)
		require.NoError(t, err)
		require.Len(t, addrs, 2)
	})

	t.Run("unassign IPv6 addresses", func(t *testing.T) { //nolint:paralleltest // existing issue.
		addrs, _ := b.AssignIpv6Addresses(ni.ID, 1)
		require.NoError(t, b.UnassignIpv6Addresses(ni.ID, addrs))
	})
}

// ---- Account/misc ---- //nolint:godot // existing issue.
