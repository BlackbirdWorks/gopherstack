package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- VPN ---- //nolint:godot // existing issue.
func TestVpnConnectionRoute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	// Create a VPN connection first
	gw, _ := b.CreateCustomerGateway("ipsec.1", "1.2.3.4", "65000")
	vgw, _ := b.CreateVpnGateway("ipsec.1")
	conn, setupErr := b.CreateVpnConnection("ipsec.1", gw.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, setupErr)

	t.Run("create route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		route, err := b.CreateVpnConnectionRoute(conn.VpnConnectionID, "192.168.0.0/24")
		require.NoError(t, err)
		assert.Equal(t, "active", route.State)
	})

	t.Run("delete route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVpnConnectionRoute(conn.VpnConnectionID, "192.168.0.0/24"))
	})

	t.Run("modify VPN connection", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyVpnConnection(conn.VpnConnectionID, ""))
	})
}

// ---- ModifyTransitGateway ---- //nolint:godot // existing issue.
