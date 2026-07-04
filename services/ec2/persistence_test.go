package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ec2.InMemoryBackend) string
		verify func(t *testing.T, b *ec2.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *ec2.InMemoryBackend) string {
				sg, err := b.CreateSecurityGroup("test-sg", "test security group", "")
				if err != nil {
					return ""
				}

				return sg.ID
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()

				sgs := b.DescribeSecurityGroups([]string{id})
				require.Len(t, sgs, 1)
				assert.Equal(t, "test-sg", sgs[0].Name)
				assert.Equal(t, id, sgs[0].ID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ec2.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ec2.InMemoryBackend, _ string) {
				t.Helper()

				sgs := b.DescribeSecurityGroups(nil)
				// Default security groups may exist from initDefaults; just verify restore worked
				assert.NotNil(t, sgs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestPersistenceNewTypes(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Add spot requests and placement groups
	req, err := b.RequestSpotInstances("ami-123", "t2.micro", "", "0.01")
	require.NoError(t, err)

	_, err = b.CreatePlacementGroup("persist-pg", "cluster")
	require.NoError(t, err)

	eni, err := b.CreateNetworkInterface("subnet-default", "persist-eni")
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(snap))

	// Verify spot requests persisted
	reqs := b2.DescribeSpotInstanceRequests([]string{req.ID})
	require.Len(t, reqs, 1)
	assert.Equal(t, req.ID, reqs[0].ID)

	// Verify placement groups persisted
	pgs := b2.DescribePlacementGroups([]string{"persist-pg"})
	require.Len(t, pgs, 1)
	assert.Equal(t, "persist-pg", pgs[0].Name)

	// Verify ENIs persisted
	enis := b2.DescribeNetworkInterfaces([]string{eni.ID})
	require.Len(t, enis, 1)
	assert.Equal(t, "persist-eni", enis[0].Description)
}

// TestPersistenceExtended verifies that §5 expansion fields survive snapshot/restore.
func TestPersistenceExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ec2.InMemoryBackend)
		verify func(t *testing.T, b *ec2.InMemoryBackend)
		name   string
	}{
		{
			name: "vpn_gateway_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateVpnGateway("ipsec.1")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				gws := b.DescribeVpnGateways(nil)
				assert.NotEmpty(t, gws)
			},
		},
		{
			name: "customer_gateway_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateCustomerGateway("ipsec.1", "1.2.3.4", "65000")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				cgws := b.DescribeCustomerGateways(nil)
				assert.NotEmpty(t, cgws)
			},
		},
		{
			name: "ipam_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateIpam()
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				ipams := b.DescribeIpams(nil)
				assert.NotEmpty(t, ipams)
			},
		},
		{
			name: "carrier_gateway_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateCarrierGateway("vpc-test")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				cgws := b.DescribeCarrierGateways(nil)
				assert.NotEmpty(t, cgws)
			},
		},
		{
			name: "ec2_fleet_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateFleet("instant", 1)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				fleets := b.DescribeFleets(nil)
				assert.NotEmpty(t, fleets)
			},
		},
		{
			name: "network_insights_path_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateNetworkInsightsPath("i-src", "i-dst", "tcp", 0)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				paths := b.DescribeNetworkInsightsPaths(nil)
				assert.NotEmpty(t, paths)
			},
		},
		{
			name: "ebs_encryption_default_persists",
			setup: func(b *ec2.InMemoryBackend) {
				b.EnableEbsEncryptionByDefault()
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				assert.True(t, b.GetEbsEncryptionByDefault())
			},
		},
		{
			name: "serial_console_access_persists",
			setup: func(b *ec2.InMemoryBackend) {
				b.EnableSerialConsoleAccess()
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				assert.True(t, b.GetSerialConsoleAccessStatus())
			},
		},
		{
			name: "managed_prefix_list_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateManagedPrefixList("persist-pl", "IPv4", 10)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				pls := b.DescribeManagedPrefixLists(nil)
				assert.NotEmpty(t, pls)
			},
		},
		{
			name: "route_server_persists",
			setup: func(b *ec2.InMemoryBackend) {
				_, err := b.CreateRouteServer(65000, "enabled", 60, false)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				servers := b.DescribeRouteServers(nil)
				assert.NotEmpty(t, servers)
			},
		},
		{
			name: "route_server_endpoint_persists",
			setup: func(b *ec2.InMemoryBackend) {
				rs, err := b.CreateRouteServer(65000, "enabled", 60, false)
				require.NoError(t, err)
				_, err = b.CreateRouteServerEndpoint(rs.RouteServerID, "subnet-default")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				endpoints := b.DescribeRouteServerEndpoints(nil)
				assert.NotEmpty(t, endpoints)
			},
		},
		{
			name: "route_server_peer_persists",
			setup: func(b *ec2.InMemoryBackend) {
				rs, err := b.CreateRouteServer(65000, "enabled", 60, false)
				require.NoError(t, err)
				ep, err := b.CreateRouteServerEndpoint(rs.RouteServerID, "subnet-default")
				require.NoError(t, err)
				_, err = b.CreateRouteServerPeer(ep.RouteServerEndpointID, "10.0.0.5", 65001, "bfd")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				peers := b.DescribeRouteServerPeers(nil)
				assert.NotEmpty(t, peers)
			},
		},
		{
			name: "route_server_association_persists",
			setup: func(b *ec2.InMemoryBackend) {
				rs, err := b.CreateRouteServer(65000, "enabled", 60, false)
				require.NoError(t, err)
				_, err = b.AssociateRouteServer(rs.RouteServerID, "vpc-default")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				servers := b.DescribeRouteServers(nil)
				require.NotEmpty(t, servers)
				assocs := b.GetRouteServerAssociations(servers[0].RouteServerID)
				assert.NotEmpty(t, assocs)
			},
		},
		{
			name: "route_server_propagation_persists",
			setup: func(b *ec2.InMemoryBackend) {
				rs, err := b.CreateRouteServer(65000, "enabled", 60, false)
				require.NoError(t, err)
				rt, err := b.CreateRouteTable("vpc-default")
				require.NoError(t, err)
				_, err = b.EnableRouteServerPropagation(rs.RouteServerID, rt.ID)
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()
				servers := b.DescribeRouteServers(nil)
				require.NotEmpty(t, servers)
				props := b.GetRouteServerPropagations(servers[0].RouteServerID)
				assert.NotEmpty(t, props)
			},
		},
		{
			name: "vpn_connection_tunnels_and_routes_persist",
			setup: func(b *ec2.InMemoryBackend) {
				cgw, err := b.CreateCustomerGateway("ipsec.1", "1.2.3.4", "65000")
				require.NoError(t, err)

				vgw, err := b.CreateVpnGateway("ipsec.1")
				require.NoError(t, err)

				conn, err := b.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, vgw.VpnGatewayID)
				require.NoError(t, err)

				_, err = b.CreateVpnConnectionRoute(conn.VpnConnectionID, "192.168.50.0/24")
				require.NoError(t, err)
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend) {
				t.Helper()

				conns := b.DescribeVpnConnections(nil)
				require.Len(t, conns, 1)
				require.Len(t, conns[0].Options.TunnelOptions, 2)
				assert.NotEmpty(t, conns[0].Options.TunnelOptions[0].OutsideIPAddress)
				assert.NotEmpty(t, conns[0].Options.TunnelOptions[0].PreSharedKey)
				require.Len(t, conns[0].VgwTelemetry, 2)

				routes := b.GetVpnConnectionRoutes(conns[0].VpnConnectionID)
				require.Len(t, routes, 1)
				assert.Equal(t, "192.168.50.0/24", routes[0].DestinationCIDR)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh)
		})
	}
}
