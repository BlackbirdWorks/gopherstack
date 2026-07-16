package ec2_test

import (
	"net/url"
	"strings"
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
		{
			name: "verified_access_policy_and_fpga_image_round_trip",
			setup: func(b *ec2.InMemoryBackend) string {
				inst, err := b.CreateVerifiedAccessInstance("persist test")
				if err != nil {
					return ""
				}
				grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "persist group")
				if err != nil {
					return ""
				}
				ep, err := b.CreateVerifiedAccessEndpoint(grp.VerifiedAccessGroupID, "load-balancer", "persist ep")
				if err != nil {
					return ""
				}
				_, modEpErr := b.ModifyVerifiedAccessEndpointPolicy(
					ep.VerifiedAccessEndpointID,
					true,
					"doc",
				)
				if modEpErr != nil {
					return ""
				}

				_, modGrpErr := b.ModifyVerifiedAccessGroupPolicy(grp.VerifiedAccessGroupID, true, "doc")
				if modGrpErr != nil {
					return ""
				}

				_, modLogErr := b.ModifyVerifiedAccessInstanceLoggingConfiguration(
					inst.VerifiedAccessInstanceID,
					ec2.VerifiedAccessLogOptions{LogVersion: "ocsf-1.0.0-rc.2"},
				)
				if modLogErr != nil {
					return ""
				}

				img, err := b.CreateFpgaImage("persist-afi", "persist description")
				if err != nil {
					return ""
				}

				return ep.VerifiedAccessEndpointID + "|" + grp.VerifiedAccessGroupID + "|" +
					inst.VerifiedAccessInstanceID + "|" + img.FpgaImageID
			},
			verify: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()

				parts := strings.Split(id, "|")
				require.Len(t, parts, 4)
				epID, grpID, instID, afiID := parts[0], parts[1], parts[2], parts[3]

				pol, err := b.GetVerifiedAccessEndpointPolicy(epID)
				require.NoError(t, err)
				assert.True(t, pol.PolicyEnabled)

				grpPol, err := b.GetVerifiedAccessGroupPolicy(grpID)
				require.NoError(t, err)
				assert.True(t, grpPol.PolicyEnabled)

				cfgs := b.DescribeVerifiedAccessInstanceLoggingConfigurations([]string{instID})
				require.Len(t, cfgs, 1)
				assert.Equal(t, "ocsf-1.0.0-rc.2", cfgs[0].AccessLogs.LogVersion)

				imgs := b.DescribeFpgaImages([]string{afiID})
				require.Len(t, imgs, 1)
				assert.Equal(t, "persist-afi", imgs[0].Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
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

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

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

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}

// TestDeleteVpc_SecondaryIndexes verifies that DeleteVpc correctly removes subnet,
// route table, and security group secondary index entries so they don't linger.
func TestDeleteVpc_SecondaryIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		addSubnet     bool
		addRouteTable bool
	}{
		{
			name:          "vpc with subnet and route table",
			addSubnet:     true,
			addRouteTable: true,
		},
		{
			name:          "vpc with only subnet",
			addSubnet:     true,
			addRouteTable: false,
		},
		{
			name:          "empty vpc",
			addSubnet:     false,
			addRouteTable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
			h := newTestHandlerWithBackend(b)

			// Create a new VPC.
			resp, err := dispatchHandler(h, url.Values{
				"Action":    {"CreateVpc"},
				"Version":   {"2016-11-15"},
				"CidrBlock": {"10.1.0.0/16"},
			})
			require.NoError(t, err)
			vpcID := accuracyExtractXMLValue(resp, "vpcId")
			require.NotEmpty(t, vpcID)

			if tc.addSubnet {
				_, err = dispatchHandler(h, url.Values{
					"Action":           {"CreateSubnet"},
					"Version":          {"2016-11-15"},
					"VpcId":            {vpcID},
					"CidrBlock":        {"10.1.1.0/24"},
					"AvailabilityZone": {"us-east-1a"},
				})
				require.NoError(t, err)
			}

			if tc.addRouteTable {
				_, err = dispatchHandler(h, url.Values{
					"Action":  {"CreateRouteTable"},
					"Version": {"2016-11-15"},
					"VpcId":   {vpcID},
				})
				require.NoError(t, err)
			}

			// Add a SG to the VPC.
			_, err = dispatchHandler(h, url.Values{
				"Action":           {"CreateSecurityGroup"},
				"Version":          {"2016-11-15"},
				"GroupName":        {"test-sg"},
				"GroupDescription": {"test sg"},
				"VpcId":            {vpcID},
			})
			require.NoError(t, err)

			// Delete the VPC.
			_, err = dispatchHandler(h, url.Values{
				"Action":  {"DeleteVpc"},
				"Version": {"2016-11-15"},
				"VpcId":   {vpcID},
			})
			require.NoError(t, err)

			// After deletion, DescribeSubnets must not return any subnet for
			// the deleted VPC — the index must be cleared.
			subnets := b.DescribeSubnetsByVPC(vpcID)
			assert.Empty(t, subnets, "no subnets should remain after DeleteVpc")

			// DescribeVpcs must not return the deleted VPC.
			vpcs := b.DescribeVpcs([]string{vpcID})
			assert.Empty(t, vpcs, "VPC must not exist after DeleteVpc")
		})
	}
}

// TestReconcileLifecycle_SkipsNonTransitional verifies that reconcileInstanceLifecycle
// does not alter instances already in stable states (stopped, terminated, running).

// TestPersistence_SecondaryIndexRebuild is the guard test for the Restore
// secondary-index rebuild. It creates a VPC with instances and ENIs, snapshots,
// restores into a fresh backend, and asserts that (a) DeleteVpc still cascades
// (proving instanceIDsByVPC / subnetIDsByVPC / natGatewayIDsByVPC / eniIDsByVPC
// were rebuilt) and (b) ENI-by-instance termination cleanup still works
// (proving eniIDsByInstance was rebuilt).
func TestPersistence_SecondaryIndexRebuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, fresh *ec2.InMemoryBackend, res vpcResources)
		name   string
	}{
		{
			name: "delete_vpc_cascade_survives_restore",
			verify: func(t *testing.T, fresh *ec2.InMemoryBackend, res vpcResources) {
				t.Helper()

				require.NoError(t, fresh.DeleteVpc(res.vpcID))

				// VPC gone.
				assert.Empty(t, fresh.DescribeVpcs([]string{res.vpcID}))
				// Subnet cascaded (index-driven).
				assert.Empty(t, fresh.DescribeSubnets([]string{res.subnetID}))
				// NAT gateways cascaded (natGatewayIDsByVPC-driven).
				assert.Empty(t, fresh.DescribeNatGateways(nil))
				// Standalone ENI cascaded (eniIDsByVPC-driven).
				assert.Empty(t, fresh.DescribeNetworkInterfaces([]string{res.eniID}))
				// Instances terminated (instanceIDsByVPC-driven).
				insts := fresh.DescribeInstances(res.instanceIDs, "")
				require.Len(t, insts, len(res.instanceIDs))
				for _, inst := range insts {
					assert.Equal(t, ec2.StateTerminated, inst.State)
				}
			},
		},
		{
			name: "eni_by_instance_cleanup_survives_restore",
			verify: func(t *testing.T, fresh *ec2.InMemoryBackend, res vpcResources) {
				t.Helper()

				// Each instance has exactly one auto-attached ENI; before
				// termination they must exist.
				before := fresh.DescribeNetworkInterfaces(nil)
				require.NotEmpty(t, before)

				// Terminating uses eniIDsByInstance to delete attached ENIs. If
				// the index was not rebuilt on restore this is a no-op and the
				// ENIs leak.
				_, err := fresh.TerminateInstances(res.instanceIDs)
				require.NoError(t, err)

				remaining := fresh.DescribeNetworkInterfaces(nil)
				for _, eni := range remaining {
					for _, instID := range res.instanceIDs {
						assert.NotEqual(t, instID, eni.InstanceID,
							"instance %s still has attached ENI %s after termination", instID, eni.ID)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			res := buildVPCWithResources(t, original, "10.20.0.0/16", "us-east-1a", 2)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, res)
		})
	}
}

// TestDeleteVpc_PerVPCIndexCascade exercises the natGatewayIDsByVPC and
// eniIDsByVPC index maintenance across DeleteNatGateway, DeleteSubnet, and
// DeleteVpc so the index never drifts from the underlying maps.

// TestDeleteVpc_PerVPCIndexCascade exercises the natGatewayIDsByVPC and
// eniIDsByVPC index maintenance across DeleteNatGateway, DeleteSubnet, and
// DeleteVpc so the index never drifts from the underlying maps.
func TestDeleteVpc_PerVPCIndexCascade(t *testing.T) {
	t.Parallel()

	t.Run("explicit_nat_delete_then_delete_vpc", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		res := buildVPCWithResources(t, b, "10.30.0.0/16", "us-east-1a", 1)

		nats := b.DescribeNatGateways(nil)
		require.Len(t, nats, 1)

		// Explicitly delete the NAT — deindex must remove it from the VPC index.
		require.NoError(t, b.DeleteNatGateway(nats[0].ID))
		assert.Empty(t, b.DescribeNatGateways(nil))

		// DeleteVpc must not error or attempt to re-delete the missing NAT.
		require.NoError(t, b.DeleteVpc(res.vpcID))
		assert.Empty(t, b.DescribeVpcs([]string{res.vpcID}))
	})

	t.Run("delete_subnet_scopes_nat_and_eni_removal", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

		vpc, err := b.CreateVpc("10.40.0.0/16")
		require.NoError(t, err)

		subnetA, err := b.CreateSubnet(vpc.ID, "10.40.1.0/24", "us-east-1a")
		require.NoError(t, err)
		subnetB, err := b.CreateSubnet(vpc.ID, "10.40.2.0/24", "us-east-1b")
		require.NoError(t, err)

		eniA, err := b.CreateNetworkInterface(subnetA.ID, "eni-a")
		require.NoError(t, err)
		eniB, err := b.CreateNetworkInterface(subnetB.ID, "eni-b")
		require.NoError(t, err)

		// Delete subnet A — only eni-a removed; eni-b (same VPC) survives.
		require.NoError(t, b.DeleteSubnet(subnetA.ID))
		assert.Empty(t, b.DescribeNetworkInterfaces([]string{eniA.ID}))
		require.Len(t, b.DescribeNetworkInterfaces([]string{eniB.ID}), 1)

		// DeleteVpc removes the remaining subnet-B ENI via the VPC index.
		require.NoError(t, b.DeleteVpc(vpc.ID))
		assert.Empty(t, b.DescribeNetworkInterfaces([]string{eniB.ID}))
	})
}

// TestModifyInstanceAttribute_Validation covers the handler-level attribute
// selection and stopped-state guard rules for ModifyInstanceAttribute.

// TestPagination_ForgedTokenRejected asserts that a forged/tampered NextToken is
// rejected with InvalidPaginationToken across the three opaque-token describe
// operations, rather than silently re-paging from offset 0.
func TestPagination_ForgedTokenRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "describe_instances", action: "DescribeInstances"},
		{name: "describe_images", action: "DescribeImages"},
		{name: "describe_instance_types", action: "DescribeInstanceTypes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			params := url.Values{
				"Action":     {tt.action},
				"Version":    {"2016-11-15"},
				"MaxResults": {"5"},
				"NextToken":  {"this-is-a-forged-token"},
			}

			_, err := dispatchHandler(h, params)
			require.Error(t, err)
			assert.ErrorIs(t, err, ec2.ErrInvalidPaginationToken)
		})
	}
}

// cloneValues returns a shallow copy of the url.Values so parallel subtests can
// mutate InstanceId without racing on the shared table entry.

// buildVPCWithResources creates a VPC with a subnet, `numInstances` running
// instances (each with an auto-attached ENI), one standalone ENI, and one NAT
// gateway, returning the created resource identifiers for later assertions.
func buildVPCWithResources(
	t *testing.T,
	b *ec2.InMemoryBackend,
	cidr, az string,
	numInstances int,
) vpcResources {
	t.Helper()

	vpc, err := b.CreateVpc(cidr)
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, cidr, az)
	require.NoError(t, err)

	instances, err := b.RunInstances("ami-123", "t3.micro", subnet.ID, numInstances)
	require.NoError(t, err)

	ids := make([]string, 0, len(instances))
	for _, inst := range instances {
		ids = append(ids, inst.ID)
	}

	eni, err := b.CreateNetworkInterface(subnet.ID, "standalone-eni")
	require.NoError(t, err)

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	_, err = b.CreateNatGateway(subnet.ID, addr.AllocationID)
	require.NoError(t, err)

	return vpcResources{
		vpcID:       vpc.ID,
		subnetID:    subnet.ID,
		eniID:       eni.ID,
		instanceIDs: ids,
	}
}

// TestPersistence_SecondaryIndexRebuild is the guard test for the Restore
// secondary-index rebuild. It creates a VPC with instances and ENIs, snapshots,
// restores into a fresh backend, and asserts that (a) DeleteVpc still cascades
// (proving instanceIDsByVPC / subnetIDsByVPC / natGatewayIDsByVPC / eniIDsByVPC
// were rebuilt) and (b) ENI-by-instance termination cleanup still works
// (proving eniIDsByInstance was rebuilt).

// TestSeedHelpers verifies all seed helper functions work correctly.
func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddAddressTransferInternal(
		&ec2.AddressTransfer{PublicIP: "1.2.3.4", AllocationID: "eipalloc-1"},
	)
	b.AddCapacityReservationInternal(
		&ec2.CapacityReservation{CapacityReservationID: "cr-1", InstanceType: "t3.micro"},
	)
	b.AddTGWPeeringAttachmentInternal(&ec2.TransitGatewayPeeringAttachment{
		TransitGatewayAttachmentID: "tgw-attach-1",
		State:                      "pending-acceptance",
	})
	b.AddTGWVpcAttachmentInternal(&ec2.TransitGatewayVpcAttachment{
		TransitGatewayAttachmentID: "tgw-attach-2",
		State:                      "pending-acceptance",
	})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-1",
		RequesterVpcID:         "vpc-a",
		AccepterVpcID:          "vpc-b",
		State:                  "pending-acceptance",
	})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24", State: "provisioned"})
	b.AddVpcEndpointConnectionInternal(&ec2.VpcEndpointConnection{
		ServiceID:     "vpce-svc-1",
		VpcEndpointID: "vpce-1",
		State:         "pending-acceptance",
	})
	b.AddTGWMulticastDomainAssociationInternal(&ec2.TransitGatewayMulticastDomainAssociation{
		TransitGatewayMulticastDomainID: "tgw-mcast-1",
		SubnetID:                        "subnet-1",
		State:                           "associated",
	})

	assert.Equal(t, 1, b.AddressTransferCount())
	assert.Equal(t, 1, b.CapacityReservationCount())
	assert.Equal(t, 1, b.TGWPeeringAttachmentCount())
	assert.Equal(t, 1, b.TGWVpcAttachmentCount())
	assert.Equal(t, 1, b.VpcPeeringConnectionCount())
	assert.Equal(t, 1, b.ByoipCidrCount())
	assert.Equal(t, 1, b.VpcEndpointConnectionCount())
	assert.Equal(t, 1, b.TGWMulticastDomainAssociationCount())
}

// TestExportCountHelpers verifies count helpers return 0 on fresh backend.

// TestExportCountHelpers verifies count helpers return 0 on fresh backend.
func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	assert.Equal(t, 0, b.AddressTransferCount())
	assert.Equal(t, 0, b.CapacityReservationCount())
	assert.Equal(t, 0, b.ReservedInstancesExchangeCount())
	assert.Equal(t, 0, b.TGWMulticastDomainAssociationCount())
	assert.Equal(t, 0, b.TGWPeeringAttachmentCount())
	assert.Equal(t, 0, b.TGWVpcAttachmentCount())
	assert.Equal(t, 0, b.VpcEndpointConnectionCount())
	assert.Equal(t, 0, b.VpcPeeringConnectionCount())
	assert.Equal(t, 0, b.ByoipCidrCount())
	assert.Equal(t, 0, b.DedicatedHostCount())
}

// TestSeedHelpers_DeepCopy verifies that seed helpers copy the value so
// mutations to the original struct do not affect the stored entry.

// TestSeedHelpers_DeepCopy verifies that seed helpers copy the value so
// mutations to the original struct do not affect the stored entry.
func TestSeedHelpers_DeepCopy(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	original := &ec2.VpcPeeringConnection{
		VpcPeeringConnectionID: "pcx-1",
		State:                  "pending-acceptance",
	}
	b.AddVpcPeeringConnectionInternal(original)

	// Mutate the original; the stored value should not change.
	original.State = "mutated"

	conns := b.DescribeVpcPeeringConnections([]string{"pcx-1"})
	require.Len(t, conns, 1)
	assert.Equal(t, "pending-acceptance", conns[0].State, "seed helper should deep-copy")
}

// TestCapacityReservationOwnedBy verifies OwnedBy is populated.

// TestPersistenceRoundTrip verifies new resource maps survive Snapshot/Restore.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddAddressTransferInternal(
		&ec2.AddressTransfer{PublicIP: "1.2.3.4", AllocationID: "eipalloc-1"},
	)
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8", State: "advertised"})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-1"})

	_, err := b.AllocateHosts("us-east-1a", "t3.micro", 1)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.AddressTransferCount())
	assert.Equal(t, 1, b2.CapacityReservationCount())
	assert.Equal(t, 1, b2.ByoipCidrCount())
	assert.Equal(t, 1, b2.VpcPeeringConnectionCount())
	assert.Equal(t, 1, b2.DedicatedHostCount())
}
