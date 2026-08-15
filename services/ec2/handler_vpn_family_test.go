package ec2_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// setupVpnConnection creates a customer gateway, VPN gateway, and VPN connection at the
// backend level, returning the connection for further mutation in tests.
func setupVpnConnection(t *testing.T, bk *ec2.InMemoryBackend) *ec2.VpnConnection {
	t.Helper()

	cgw, err := bk.CreateCustomerGateway("ipsec.1", "1.2.3.4", "65000")
	require.NoError(t, err)

	vgw, err := bk.CreateVpnGateway("ipsec.1")
	require.NoError(t, err)

	conn, err := bk.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, err)

	return conn
}

// TestCreateVpnConnection_TunnelsAndTelemetry verifies that a newly-created VPN connection
// gets two fully-populated IPsec tunnels and matching VGW telemetry entries.
func TestCreateVpnConnection_TunnelsAndTelemetry(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	conn := setupVpnConnection(t, bk)

	assert.Equal(t, "VPN", conn.Category)
	assert.NotEmpty(t, conn.CustomerGatewayConfiguration)
	assert.Contains(t, conn.CustomerGatewayConfiguration, conn.VpnConnectionID)

	require.Len(t, conn.Options.TunnelOptions, 2)
	require.Len(t, conn.VgwTelemetry, 2)

	seen := map[string]bool{}

	for i, tun := range conn.Options.TunnelOptions {
		assert.NotEmpty(t, tun.OutsideIPAddress)
		assert.False(t, seen[tun.OutsideIPAddress], "tunnel outside IPs must be distinct")
		seen[tun.OutsideIPAddress] = true

		assert.NotEmpty(t, tun.TunnelInsideCIDR)
		assert.NotEmpty(t, tun.PreSharedKey)
		assert.Positive(t, tun.Phase1LifetimeSeconds)
		assert.Positive(t, tun.Phase2LifetimeSeconds)
		assert.Contains(t, tun.IKEVersions, "ikev2")

		telem := conn.VgwTelemetry[i]
		assert.Equal(t, tun.OutsideIPAddress, telem.OutsideIPAddress)
		assert.Equal(t, "DOWN", telem.Status)
		assert.NotEmpty(t, telem.LastStatusChange)
	}
}

// TestModifyVpnConnectionOptions_Backend table-drives the option-merging semantics of
// ModifyVpnConnectionOptions: empty/nil inputs must leave existing fields untouched.
func TestModifyVpnConnectionOptions_Backend(t *testing.T) {
	t.Parallel()

	trueVal := true

	tests := []struct {
		name           string
		localCIDR      string
		remoteCIDR     string
		staticOnly     *bool
		wantLocalCIDR  string
		wantRemoteCIDR string
		wantStaticOnly bool
	}{
		{
			name:           "sets local and remote CIDRs and static routes only",
			localCIDR:      "10.0.0.0/16",
			remoteCIDR:     "192.168.0.0/16",
			staticOnly:     &trueVal,
			wantLocalCIDR:  "10.0.0.0/16",
			wantRemoteCIDR: "192.168.0.0/16",
			wantStaticOnly: true,
		},
		{
			name:           "empty inputs leave fields unchanged",
			localCIDR:      "",
			remoteCIDR:     "",
			staticOnly:     nil,
			wantLocalCIDR:  "",
			wantRemoteCIDR: "",
			wantStaticOnly: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newTestBackend()
			conn := setupVpnConnection(t, bk)

			updated, err := bk.ModifyVpnConnectionOptions(
				conn.VpnConnectionID, tt.localCIDR, tt.remoteCIDR, tt.staticOnly,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantLocalCIDR, updated.Options.LocalIPv4NetworkCIDR)
			assert.Equal(t, tt.wantRemoteCIDR, updated.Options.RemoteIPv4NetworkCIDR)
			assert.Equal(t, tt.wantStaticOnly, updated.Options.StaticRoutesOnly)
		})
	}

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		bk := newTestBackend()
		_, err := bk.ModifyVpnConnectionOptions("vpn-nonexistent", "10.0.0.0/16", "", nil)
		require.ErrorIs(t, err, ec2.ErrVpnConnectionNotFound)
	})
}

// TestModifyVpnTunnelOptions_Backend verifies tunnel-level option mutation and error handling.
func TestModifyVpnTunnelOptions_Backend(t *testing.T) {
	t.Parallel()

	t.Run("updates the matching tunnel only", func(t *testing.T) {
		t.Parallel()

		bk := newTestBackend()
		conn := setupVpnConnection(t, bk)
		targetIP := conn.Options.TunnelOptions[0].OutsideIPAddress
		otherIP := conn.Options.TunnelOptions[1].OutsideIPAddress

		updated, err := bk.ModifyVpnTunnelOptions(conn.VpnConnectionID, targetIP, ec2.VpnTunnelOptionsModify{
			PreSharedKey:          "supersecretkey1234567890",
			Phase1LifetimeSeconds: 3600,
		})
		require.NoError(t, err)

		var target, other ec2.VpnTunnelOption
		for _, tun := range updated.Options.TunnelOptions {
			if tun.OutsideIPAddress == targetIP {
				target = tun
			}
			if tun.OutsideIPAddress == otherIP {
				other = tun
			}
		}

		assert.Equal(t, "supersecretkey1234567890", target.PreSharedKey)
		assert.EqualValues(t, 3600, target.Phase1LifetimeSeconds)
		assert.NotEqual(t, "supersecretkey1234567890", other.PreSharedKey, "other tunnel must be untouched")
	})

	t.Run("unknown tunnel outside IP returns error", func(t *testing.T) {
		t.Parallel()

		bk := newTestBackend()
		conn := setupVpnConnection(t, bk)

		_, err := bk.ModifyVpnTunnelOptions(conn.VpnConnectionID, "9.9.9.9", ec2.VpnTunnelOptionsModify{})
		require.ErrorIs(t, err, ec2.ErrVpnTunnelNotFound)
	})

	t.Run("unknown connection returns error", func(t *testing.T) {
		t.Parallel()

		bk := newTestBackend()

		_, err := bk.ModifyVpnTunnelOptions("vpn-nonexistent", "9.9.9.9", ec2.VpnTunnelOptionsModify{})
		require.ErrorIs(t, err, ec2.ErrVpnConnectionNotFound)
	})
}

// TestModifyVpnTunnelCertificate_Backend verifies certificate provisioning updates both the
// tunnel option and its corresponding VGW telemetry entry.
func TestModifyVpnTunnelCertificate_Backend(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	conn := setupVpnConnection(t, bk)
	targetIP := conn.Options.TunnelOptions[0].OutsideIPAddress

	updated, err := bk.ModifyVpnTunnelCertificate(conn.VpnConnectionID, targetIP)
	require.NoError(t, err)

	assert.Contains(t, updated.Options.TunnelOptions[0].CertificateARN, "arn:aws:acm:")

	found := false
	for _, telem := range updated.VgwTelemetry {
		if telem.OutsideIPAddress == targetIP {
			assert.Equal(t, updated.Options.TunnelOptions[0].CertificateARN, telem.CertificateARN)
			found = true
		}
	}
	assert.True(t, found, "telemetry entry for target tunnel must exist")
}

// TestGetVpnConnectionDeviceTypes_Backend verifies the static device catalog is non-empty and
// each entry is fully populated.
func TestGetVpnConnectionDeviceTypes_Backend(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	types := bk.GetVpnConnectionDeviceTypes()

	require.NotEmpty(t, types)

	for _, dt := range types {
		assert.NotEmpty(t, dt.VpnConnectionDeviceTypeID)
		assert.NotEmpty(t, dt.Vendor)
		assert.NotEmpty(t, dt.Platform)
		assert.NotEmpty(t, dt.Software)
	}
}

// TestGetVpnConnectionDeviceSampleConfiguration_Backend table-drives device-type/IKE-version
// input handling, including defaulting behavior and the not-found error path.
func TestGetVpnConnectionDeviceSampleConfiguration_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		deviceTypeID string
		ikeVersion   string
	}{
		{
			name:         "explicit device type and IKE version",
			deviceTypeID: "cisco-systems-inc-cisco-ios-15",
			ikeVersion:   "ikev1",
		},
		{name: "defaults when empty", deviceTypeID: "", ikeVersion: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newTestBackend()
			conn := setupVpnConnection(t, bk)

			config, err := bk.GetVpnConnectionDeviceSampleConfiguration(
				conn.VpnConnectionID,
				tt.deviceTypeID,
				tt.ikeVersion,
			)
			require.NoError(t, err)
			assert.Contains(t, config, conn.VpnConnectionID)
			assert.Contains(t, config, conn.Options.TunnelOptions[0].OutsideIPAddress)
		})
	}

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		bk := newTestBackend()
		_, err := bk.GetVpnConnectionDeviceSampleConfiguration("vpn-nonexistent", "", "")
		require.ErrorIs(t, err, ec2.ErrVpnConnectionNotFound)
	})
}

// TestGetVpnTunnelReplacementStatus_Backend verifies the maintenance status shape and error paths.
func TestGetVpnTunnelReplacementStatus_Backend(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	conn := setupVpnConnection(t, bk)
	targetIP := conn.Options.TunnelOptions[0].OutsideIPAddress

	status, err := bk.GetVpnTunnelReplacementStatus(conn.VpnConnectionID, targetIP)
	require.NoError(t, err)
	assert.Equal(t, conn.VpnConnectionID, status.VpnConnectionID)
	assert.Equal(t, targetIP, status.VpnTunnelOutsideIPAddress)
	assert.Equal(t, "false", status.MaintenanceDetails.PendingMaintenance)

	_, err = bk.GetVpnTunnelReplacementStatus(conn.VpnConnectionID, "9.9.9.9")
	require.ErrorIs(t, err, ec2.ErrVpnTunnelNotFound)
}

// TestModifyVpnConnection_MovesGateway verifies ModifyVpnConnection re-attaches a connection to
// a different VPN Gateway and validates the target gateway's existence.
func TestModifyVpnConnection_Backend(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	conn := setupVpnConnection(t, bk)

	newVgw, err := bk.CreateVpnGateway("ipsec.1")
	require.NoError(t, err)

	require.NoError(t, bk.ModifyVpnConnection(conn.VpnConnectionID, newVgw.VpnGatewayID))

	found := bk.DescribeVpnConnections([]string{conn.VpnConnectionID})
	require.Len(t, found, 1)
	assert.Equal(t, newVgw.VpnGatewayID, found[0].VpnGatewayID)

	err = bk.ModifyVpnConnection(conn.VpnConnectionID, "vgw-nonexistent")
	require.ErrorIs(t, err, ec2.ErrVpnGatewayNotFound)

	err = bk.ModifyVpnConnection("vpn-nonexistent", "")
	require.ErrorIs(t, err, ec2.ErrVpnConnectionNotFound)
}

// TestDeleteVpnConnection_CleansUpRoutes verifies that deleting a VPN connection removes its
// associated static routes.
func TestDeleteVpnConnection_CleansUpRoutes(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	conn := setupVpnConnection(t, bk)

	_, err := bk.CreateVpnConnectionRoute(conn.VpnConnectionID, "192.168.100.0/24")
	require.NoError(t, err)
	require.Len(t, bk.GetVpnConnectionRoutes(conn.VpnConnectionID), 1)

	require.NoError(t, bk.DeleteVpnConnection(conn.VpnConnectionID))
	assert.Empty(t, bk.GetVpnConnectionRoutes(conn.VpnConnectionID))
}

// TestVpnConnectionHandlers_XMLShapes drives the full VPN connection family through the HTTP
// handler and asserts the wire-level XML shapes for the newly-implemented operations.
func TestVpnConnectionHandlers_XMLShapes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	cgwRec := postForm(
		t,
		h,
		"Action=CreateCustomerGateway&Version=2016-11-15&Type=ipsec.1&IpAddress=9.8.7.6&BgpAsn=65000",
	)
	require.Equal(t, http.StatusOK, cgwRec.Code)

	var cgwResp struct {
		XMLName         xml.Name `xml:"CreateCustomerGatewayResponse"`
		CustomerGateway struct {
			CustomerGatewayID string `xml:"customerGatewayId"`
		} `xml:"customerGateway"`
	}
	require.NoError(t, xml.Unmarshal(cgwRec.Body.Bytes(), &cgwResp))
	cgwID := cgwResp.CustomerGateway.CustomerGatewayID
	require.NotEmpty(t, cgwID)

	vgwRec := postForm(t, h, "Action=CreateVpnGateway&Version=2016-11-15&Type=ipsec.1")
	require.Equal(t, http.StatusOK, vgwRec.Code)

	var vgwResp struct {
		XMLName    xml.Name `xml:"CreateVpnGatewayResponse"`
		VpnGateway struct {
			VpnGatewayID string `xml:"vpnGatewayId"`
		} `xml:"vpnGateway"`
	}
	require.NoError(t, xml.Unmarshal(vgwRec.Body.Bytes(), &vgwResp))
	vgwID := vgwResp.VpnGateway.VpnGatewayID
	require.NotEmpty(t, vgwID)

	connRec := postForm(t, h, "Action=CreateVpnConnection&Version=2016-11-15&Type=ipsec.1"+
		"&CustomerGatewayId="+cgwID+"&VpnGatewayId="+vgwID)
	require.Equal(t, http.StatusOK, connRec.Code)

	var connResp struct {
		XMLName       xml.Name `xml:"CreateVpnConnectionResponse"`
		Xmlns         string   `xml:"xmlns,attr"`
		RequestID     string   `xml:"requestId"`
		VpnConnection struct {
			VpnConnectionID string `xml:"vpnConnectionId"`
			State           string `xml:"state"`
			Options         struct {
				TunnelOptionsSet struct {
					Items []struct {
						OutsideIPAddress string `xml:"outsideIpAddress"`
					} `xml:"item"`
				} `xml:"tunnelOptionSet"`
			} `xml:"options"`
			VgwTelemetrySet struct {
				Items []struct {
					OutsideIPAddress string `xml:"outsideIpAddress"`
					Status           string `xml:"status"`
				} `xml:"item"`
			} `xml:"vgwTelemetry"`
		} `xml:"vpnConnection"`
	}
	require.NoError(t, xml.Unmarshal(connRec.Body.Bytes(), &connResp))
	assert.NotEmpty(t, connResp.Xmlns)
	assert.NotEmpty(t, connResp.RequestID)
	assert.Equal(t, "available", connResp.VpnConnection.State)
	require.Len(t, connResp.VpnConnection.Options.TunnelOptionsSet.Items, 2)
	require.Len(t, connResp.VpnConnection.VgwTelemetrySet.Items, 2)
	assert.Equal(t, "DOWN", connResp.VpnConnection.VgwTelemetrySet.Items[0].Status)

	connID := connResp.VpnConnection.VpnConnectionID
	outsideIP := connResp.VpnConnection.Options.TunnelOptionsSet.Items[0].OutsideIPAddress

	// ---- ModifyVpnConnectionOptions ----
	modOptRec := postForm(t, h, "Action=ModifyVpnConnectionOptions&Version=2016-11-15"+
		"&VpnConnectionId="+connID+"&LocalIpv4NetworkCidr=10.1.0.0/16")
	require.Equal(t, http.StatusOK, modOptRec.Code)

	var modOptResp struct {
		XMLName       xml.Name `xml:"ModifyVpnConnectionOptionsResponse"`
		VpnConnection struct {
			Options struct {
				LocalIpv4NetworkCidr string `xml:"localIpv4NetworkCidr"`
			} `xml:"options"`
		} `xml:"vpnConnection"`
	}
	require.NoError(t, xml.Unmarshal(modOptRec.Body.Bytes(), &modOptResp))
	assert.Equal(t, "10.1.0.0/16", modOptResp.VpnConnection.Options.LocalIpv4NetworkCidr)

	// ---- ModifyVpnTunnelOptions ----
	modTunRec := postForm(t, h, "Action=ModifyVpnTunnelOptions&Version=2016-11-15"+
		"&VpnConnectionId="+connID+"&VpnTunnelOutsideIpAddress="+outsideIP+
		"&TunnelOptions.PreSharedKey=abcdefghij1234567890")
	require.Equal(t, http.StatusOK, modTunRec.Code)
	assert.Contains(t, modTunRec.Body.String(), "abcdefghij1234567890")

	// ---- ModifyVpnTunnelCertificate ----
	modCertRec := postForm(t, h, "Action=ModifyVpnTunnelCertificate&Version=2016-11-15"+
		"&VpnConnectionId="+connID+"&VpnTunnelOutsideIpAddress="+outsideIP)
	require.Equal(t, http.StatusOK, modCertRec.Code)
	assert.Contains(t, modCertRec.Body.String(), "arn:aws:acm:")

	// ---- GetVpnConnectionDeviceTypes ----
	devTypesRec := postForm(t, h, "Action=GetVpnConnectionDeviceTypes&Version=2016-11-15")
	require.Equal(t, http.StatusOK, devTypesRec.Code)

	var devTypesResp struct {
		XMLName                    xml.Name `xml:"GetVpnConnectionDeviceTypesResponse"`
		VpnConnectionDeviceTypeSet struct {
			Items []struct {
				VpnConnectionDeviceTypeID string `xml:"vpnConnectionDeviceTypeId"`
			} `xml:"item"`
		} `xml:"vpnConnectionDeviceTypeSet"`
	}
	require.NoError(t, xml.Unmarshal(devTypesRec.Body.Bytes(), &devTypesResp))
	assert.NotEmpty(t, devTypesResp.VpnConnectionDeviceTypeSet.Items)

	// ---- GetVpnConnectionDeviceSampleConfiguration ----
	sampleRec := postForm(t, h, "Action=GetVpnConnectionDeviceSampleConfiguration&Version=2016-11-15"+
		"&VpnConnectionId="+connID)
	require.Equal(t, http.StatusOK, sampleRec.Code)
	assert.Contains(t, sampleRec.Body.String(), connID)

	// ---- GetVpnTunnelReplacementStatus ----
	replRec := postForm(t, h, "Action=GetVpnTunnelReplacementStatus&Version=2016-11-15"+
		"&VpnConnectionId="+connID+"&VpnTunnelOutsideIpAddress="+outsideIP)
	require.Equal(t, http.StatusOK, replRec.Code)

	var replResp struct {
		XMLName            xml.Name `xml:"GetVpnTunnelReplacementStatusResponse"`
		MaintenanceDetails struct {
			PendingMaintenance string `xml:"pendingMaintenance"`
		} `xml:"maintenanceDetails"`
	}
	require.NoError(t, xml.Unmarshal(replRec.Body.Bytes(), &replResp))
	assert.Equal(t, "false", replResp.MaintenanceDetails.PendingMaintenance)

	// ---- ModifyVpnConnection ----
	newVgwRec := postForm(t, h, "Action=CreateVpnGateway&Version=2016-11-15&Type=ipsec.1")
	require.Equal(t, http.StatusOK, newVgwRec.Code)

	var newVgwResp struct {
		VpnGateway struct {
			VpnGatewayID string `xml:"vpnGatewayId"`
		} `xml:"vpnGateway"`
	}
	require.NoError(t, xml.Unmarshal(newVgwRec.Body.Bytes(), &newVgwResp))

	modConnRec := postForm(t, h, "Action=ModifyVpnConnection&Version=2016-11-15"+
		"&VpnConnectionId="+connID+"&VpnGatewayId="+newVgwResp.VpnGateway.VpnGatewayID)
	require.Equal(t, http.StatusOK, modConnRec.Code)

	var modConnResp struct {
		XMLName       xml.Name `xml:"ModifyVpnConnectionResponse"`
		VpnConnection struct {
			VpnGatewayID string `xml:"vpnGatewayId"`
		} `xml:"vpnConnection"`
	}
	require.NoError(t, xml.Unmarshal(modConnRec.Body.Bytes(), &modConnResp))
	assert.Equal(t, newVgwResp.VpnGateway.VpnGatewayID, modConnResp.VpnConnection.VpnGatewayID)

	// ---- CreateVpnConnectionRoute wire shape ----
	routeRec := postForm(t, h, "Action=CreateVpnConnectionRoute&Version=2016-11-15"+
		"&VpnConnectionId="+connID+"&DestinationCidrBlock=172.16.0.0/24")
	require.Equal(t, http.StatusOK, routeRec.Code)

	var routeResp struct {
		XMLName xml.Name `xml:"CreateVpnConnectionRouteResponse"`
		Return  bool     `xml:"return"`
	}
	require.NoError(t, xml.Unmarshal(routeRec.Body.Bytes(), &routeResp))
	assert.True(t, routeResp.Return)

	// ---- DescribeVpnConnections reflects the route ----
	descRec := postForm(t, h, "Action=DescribeVpnConnections&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		VpnConnectionSet struct {
			Items []struct {
				RoutesSet struct {
					Items []struct {
						DestinationCidrBlock string `xml:"destinationCidrBlock"`
					} `xml:"item"`
				} `xml:"routes"`
			} `xml:"item"`
		} `xml:"vpnConnectionSet"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
	require.Len(t, descResp.VpnConnectionSet.Items, 1)
	require.Len(t, descResp.VpnConnectionSet.Items[0].RoutesSet.Items, 1)
	assert.Equal(t, "172.16.0.0/24", descResp.VpnConnectionSet.Items[0].RoutesSet.Items[0].DestinationCidrBlock)
}

// TestVpnConnection_ErrorCodes verifies AWS-accurate error codes on not-found paths through the
// handler.
func TestVpnConnection_ErrorCodes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=GetVpnTunnelReplacementStatus&Version=2016-11-15"+
		"&VpnConnectionId=vpn-nonexistent&VpnTunnelOutsideIpAddress=1.2.3.4")
	assert.Contains(t, rec.Body.String(), "InvalidVpnConnectionID.NotFound")
}
