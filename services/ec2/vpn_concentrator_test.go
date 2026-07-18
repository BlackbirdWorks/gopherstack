package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestVpnConcentrator_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	t.Run("create defaults type to ipsec.1", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vc, err := b.CreateVpnConcentrator("", "", nil)
		require.NoError(t, err)
		assert.NotEmpty(t, vc.VpnConcentratorID)
		assert.Equal(t, "ipsec.1", vc.Type)
		assert.Equal(t, "available", vc.State)
		assert.Empty(t, vc.TransitGatewayID)
	})

	t.Run("create fails for unknown transit gateway", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpnConcentrator("ipsec.1", "tgw-doesnotexist", nil)
		require.Error(t, err)
	})

	var concentratorID string

	t.Run("create with real transit gateway", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tgw, err := b.CreateTransitGateway("test")
		require.NoError(t, err)

		vc, err := b.CreateVpnConcentrator("ipsec.1", tgw.ID, map[string]string{"Name": "test"})
		require.NoError(t, err)
		assert.Equal(t, tgw.ID, vc.TransitGatewayID)
		assert.NotEmpty(t, vc.TransitGatewayAttachmentID)
		assert.Equal(t, "test", vc.Tags["Name"])
		concentratorID = vc.VpnConcentratorID
	})

	t.Run("describe filters by id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vcs := b.DescribeVpnConcentrators([]string{concentratorID})
		require.Len(t, vcs, 1)
		assert.Equal(t, concentratorID, vcs[0].VpnConcentratorID)

		assert.Len(t, b.DescribeVpnConcentrators(nil), 2)
	})

	t.Run("delete removes concentrator", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vc, err := b.DeleteVpnConcentrator(concentratorID)
		require.NoError(t, err)
		assert.Equal(t, "deleted", vc.State)
		assert.Len(t, b.DescribeVpnConcentrators(nil), 1)
	})

	t.Run("delete unknown concentrator errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteVpnConcentrator(concentratorID)
		require.Error(t, err)
	})
}

func TestVpnTunnelExtras(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	cgw, err := b.CreateCustomerGateway("ipsec.1", "203.0.113.1", "65000")
	require.NoError(t, err)

	vgw, err := b.CreateVpnGateway("ipsec.1")
	require.NoError(t, err)

	conn, err := b.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, err)
	require.Len(t, conn.Options.TunnelOptions, 2)

	outsideIP := conn.Options.TunnelOptions[0].OutsideIPAddress
	originalPSK := conn.Options.TunnelOptions[0].PreSharedKey

	t.Run("get tunnel status requires a known tunnel", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, statusErr := b.GetActiveVpnTunnelStatus("vpn-doesnotexist", outsideIP)
		require.Error(t, statusErr)

		_, statusErr = b.GetActiveVpnTunnelStatus(conn.VpnConnectionID, "198.51.100.99")
		require.Error(t, statusErr)

		status, statusErr := b.GetActiveVpnTunnelStatus(conn.VpnConnectionID, outsideIP)
		require.NoError(t, statusErr)
		assert.Equal(t, "available", status.ProvisioningStatus)
		assert.NotEmpty(t, status.Phase1EncryptionAlgorithm)
	})

	t.Run("replace vpn tunnel rotates the pre-shared key", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ok, replaceErr := b.ReplaceVpnTunnel(conn.VpnConnectionID, outsideIP)
		require.NoError(t, replaceErr)
		assert.True(t, ok)

		conns := b.DescribeVpnConnections([]string{conn.VpnConnectionID})
		require.Len(t, conns, 1)
		assert.NotEqual(t, originalPSK, conns[0].Options.TunnelOptions[0].PreSharedKey)
	})

	t.Run("replace vpn tunnel requires a known tunnel", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, replaceErr := b.ReplaceVpnTunnel("vpn-doesnotexist", outsideIP)
		require.Error(t, replaceErr)

		_, replaceErr = b.ReplaceVpnTunnel(conn.VpnConnectionID, "198.51.100.99")
		require.Error(t, replaceErr)
	})
}
