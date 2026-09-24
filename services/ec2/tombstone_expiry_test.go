package ec2_test

import (
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestTombstones_ExpireAfterRetentionWindow locks in the fix for
// gopherstack's unbounded-tombstone-map leak: each of EC2's six delete-waiter
// tombstone maps (TGW route tables, TGW VPC attachments, TGW peering
// attachments, NAT gateways, Fleets, VPN connections) must keep a deleted
// resource describable by id in a "deleted*" state only within
// ec2.TombstoneTTLForTest, and stop returning it once that window has
// passed.
func TestTombstones_ExpireAfterRetentionWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create        func(t *testing.T, b *ec2.InMemoryBackend) string
		delete        func(t *testing.T, b *ec2.InMemoryBackend, id string)
		describeState func(b *ec2.InMemoryBackend, id string) (found bool, state string)
		name          string
	}{
		{
			name: "nat gateway",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				addr, err := b.AllocateAddress()
				require.NoError(t, err)
				ngw, err := b.CreateNatGateway("subnet-default", addr.AllocationID, nil)
				require.NoError(t, err)

				return ngw.ID
			},
			delete: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, b.DeleteNatGateway(id))
			},
			describeState: func(b *ec2.InMemoryBackend, id string) (bool, string) {
				out := b.DescribeNatGateways([]string{id})
				if len(out) == 0 {
					return false, ""
				}

				return true, out[0].State
			},
		},
		{
			name: "transit gateway route table",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "tombstone-test"})
				require.NoError(t, err)
				rt, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
				require.NoError(t, err)

				return rt.RouteTableID
			},
			delete: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, b.DeleteTransitGatewayRouteTable(id))
			},
			describeState: func(b *ec2.InMemoryBackend, id string) (bool, string) {
				out := b.DescribeTransitGatewayRouteTables([]string{id})
				if len(out) == 0 {
					return false, ""
				}

				return true, out[0].State
			},
		},
		{
			name: "transit gateway vpc attachment",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "tombstone-test"})
				require.NoError(t, err)
				att, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-tombstone", nil, nil)
				require.NoError(t, err)

				return att.TransitGatewayAttachmentID
			},
			delete: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, b.DeleteTransitGatewayVpcAttachment(id))
			},
			describeState: func(b *ec2.InMemoryBackend, id string) (bool, string) {
				out := b.DescribeTransitGatewayVpcAttachments([]string{id})
				if len(out) == 0 {
					return false, ""
				}

				return true, out[0].State
			},
		},
		{
			name: "transit gateway peering attachment",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "requester"})
				require.NoError(t, err)
				att, err := b.CreateTransitGatewayPeeringAttachment(
					tgw.ID, "tgw-12345678", "999999999999", "us-west-2")
				require.NoError(t, err)

				return att.TransitGatewayAttachmentID
			},
			delete: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()
				_, err := b.DeleteTransitGatewayPeeringAttachment(id)
				require.NoError(t, err)
			},
			describeState: func(b *ec2.InMemoryBackend, id string) (bool, string) {
				out := b.DescribeTransitGatewayPeeringAttachments([]string{id})
				if len(out) == 0 {
					return false, ""
				}

				return true, out[0].State
			},
		},
		{
			name: "fleet",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				f, _, err := b.CreateFleet(ec2.FleetCreateInput{TotalTargetCapacity: 1})
				require.NoError(t, err)

				return f.FleetID
			},
			delete: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()
				b.DeleteFleets([]string{id}, false)
			},
			describeState: func(b *ec2.InMemoryBackend, id string) (bool, string) {
				out := b.DescribeFleets([]string{id})
				if len(out) == 0 {
					return false, ""
				}

				return true, out[0].FleetState
			},
		},
		{
			name: "vpn connection",
			create: func(t *testing.T, b *ec2.InMemoryBackend) string {
				t.Helper()

				cgw, err := b.CreateCustomerGateway("ipsec.1", "203.0.113.9", "65000")
				require.NoError(t, err)
				vgw, err := b.CreateVpnGateway("ipsec.1", 0)
				require.NoError(t, err)
				conn, err := b.CreateVpnConnection("ipsec.1", cgw.CustomerGatewayID, vgw.VpnGatewayID)
				require.NoError(t, err)

				return conn.VpnConnectionID
			},
			delete: func(t *testing.T, b *ec2.InMemoryBackend, id string) {
				t.Helper()
				require.NoError(t, b.DeleteVpnConnection(id))
			},
			describeState: func(b *ec2.InMemoryBackend, id string) (bool, string) {
				out := b.DescribeVpnConnections([]string{id})
				if len(out) == 0 {
					return false, ""
				}

				return true, out[0].State
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

				id := tt.create(t, b)
				tt.delete(t, b, id)

				found, state := tt.describeState(b, id)
				require.True(t, found, "tombstone must still be describable within the retention window")
				assert.True(t, strings.HasPrefix(state, "deleted"), "state = %q", state)

				time.Sleep(ec2.TombstoneTTLForTest + time.Second)

				found, _ = tt.describeState(b, id)
				assert.False(t, found, "tombstone must no longer be describable past the retention window")
			})
		})
	}
}

// TestJanitor_SweepExpiredTombstones proves the janitor actually prunes
// expired tombstone entries from the backend's maps (not just filters them
// out of Describe results), which is what bounds their memory growth for a
// long-running emulator whose terraform suites create and delete thousands
// of these resources.
func TestJanitor_SweepExpiredTombstones(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		j := ec2.NewJanitor(b, time.Minute, time.Hour, 6*time.Hour)

		tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "sweep-test"})
		require.NoError(t, err)
		rt, err := b.CreateTransitGatewayRouteTable(tgw.ID, nil)
		require.NoError(t, err)
		require.NoError(t, b.DeleteTransitGatewayRouteTable(rt.RouteTableID))

		require.Equal(t, 1, b.TombstoneCountsForTest())

		j.SweepExpiredTombstonesForTest(t.Context())
		assert.Equal(t, 1, b.TombstoneCountsForTest(), "sweep must not evict a tombstone within its retention window")

		time.Sleep(ec2.TombstoneTTLForTest + time.Second)

		j.SweepExpiredTombstonesForTest(t.Context())
		assert.Equal(t, 0, b.TombstoneCountsForTest(), "sweep must evict a tombstone past its retention window")
	})
}
