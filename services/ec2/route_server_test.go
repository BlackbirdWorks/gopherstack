package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Route Server ----

func TestRouteServer_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var rsID string

	t.Run("create requires asn", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServer(0, "", 0, false)
		require.Error(t, err)
	})

	t.Run("create route server", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rs, err := b.CreateRouteServer(65000, "enabled", 60, true)
		require.NoError(t, err)
		assert.NotEmpty(t, rs.RouteServerID)
		assert.Equal(t, int64(65000), rs.AmazonSideAsn)
		assert.Equal(t, "available", rs.State)
		assert.Equal(t, "enabled", rs.PersistRoutesState)
		assert.Equal(t, int64(60), rs.PersistRoutesDuration)
		assert.True(t, rs.SnsNotificationsEnabled)
		rsID = rs.RouteServerID
	})

	t.Run("describe returns created route server", func(t *testing.T) { //nolint:paralleltest // existing issue.
		servers := b.DescribeRouteServers([]string{rsID})
		require.Len(t, servers, 1)
		assert.Equal(t, rsID, servers[0].RouteServerID)
	})

	t.Run("describe all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		servers := b.DescribeRouteServers(nil)
		assert.NotEmpty(t, servers)
	})

	t.Run("modify route server", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rs, err := b.ModifyRouteServer(rsID, "disabled", 30, false)
		require.NoError(t, err)
		assert.Equal(t, "disabled", rs.PersistRoutesState)
		assert.Equal(t, int64(30), rs.PersistRoutesDuration)
		assert.False(t, rs.SnsNotificationsEnabled)
	})

	t.Run("modify non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyRouteServer("rs-nonexistent", "enabled", 60, false)
		require.Error(t, err)
	})

	t.Run("delete route server", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rs, err := b.DeleteRouteServer(rsID)
		require.NoError(t, err)
		assert.Equal(t, "deleting", rs.State)

		servers := b.DescribeRouteServers([]string{rsID})
		assert.Empty(t, servers)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteRouteServer("rs-nonexistent")
		require.Error(t, err)
	})
}

func TestRouteServer_Endpoint(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rs, setupErr := b.CreateRouteServer(65000, "enabled", 60, false)
	require.NoError(t, setupErr)

	var epID string

	t.Run("create requires route server id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServerEndpoint("", "subnet-default")
		require.Error(t, err)
	})

	t.Run("create requires subnet id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServerEndpoint(rs.RouteServerID, "")
		require.Error(t, err)
	})

	t.Run("create with unknown route server errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServerEndpoint("rs-nonexistent", "subnet-default")
		require.Error(t, err)
	})

	t.Run("create with unknown subnet returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServerEndpoint(rs.RouteServerID, "subnet-nonexistent")
		require.Error(t, err)
	})

	t.Run("create endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.CreateRouteServerEndpoint(rs.RouteServerID, "subnet-default")
		require.NoError(t, err)
		assert.NotEmpty(t, ep.RouteServerEndpointID)
		assert.Equal(t, rs.RouteServerID, ep.RouteServerID)
		assert.Equal(t, "subnet-default", ep.SubnetID)
		assert.Equal(t, "vpc-default", ep.VpcID)
		assert.NotEmpty(t, ep.EniID)
		assert.NotEmpty(t, ep.EniAddress)
		assert.Equal(t, "available", ep.State)
		epID = ep.RouteServerEndpointID
	})

	t.Run("describe returns created endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		endpoints := b.DescribeRouteServerEndpoints([]string{epID})
		require.Len(t, endpoints, 1)
		assert.Equal(t, epID, endpoints[0].RouteServerEndpointID)
	})

	t.Run("describe all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		endpoints := b.DescribeRouteServerEndpoints(nil)
		assert.NotEmpty(t, endpoints)
	})

	t.Run("delete endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.DeleteRouteServerEndpoint(epID)
		require.NoError(t, err)
		assert.Equal(t, "deleting", ep.State)

		endpoints := b.DescribeRouteServerEndpoints([]string{epID})
		assert.Empty(t, endpoints)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteRouteServerEndpoint("rse-nonexistent")
		require.Error(t, err)
	})
}

func TestRouteServer_Peer(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rs, setupErr := b.CreateRouteServer(65000, "enabled", 60, false)
	require.NoError(t, setupErr)
	ep, setupErr := b.CreateRouteServerEndpoint(rs.RouteServerID, "subnet-default")
	require.NoError(t, setupErr)

	var peerID string

	t.Run("create requires endpoint id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServerPeer("", "10.0.0.5", 65001, "bfd")
		require.Error(t, err)
	})

	t.Run("create requires peer address", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServerPeer(ep.RouteServerEndpointID, "", 65001, "bfd")
		require.Error(t, err)
	})

	t.Run("create with unknown endpoint returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateRouteServerPeer("rse-nonexistent", "10.0.0.5", 65001, "bfd")
		require.Error(t, err)
	})

	t.Run("create peer", func(t *testing.T) { //nolint:paralleltest // existing issue.
		p, err := b.CreateRouteServerPeer(ep.RouteServerEndpointID, "10.0.0.5", 65001, "bfd")
		require.NoError(t, err)
		assert.NotEmpty(t, p.RouteServerPeerID)
		assert.Equal(t, ep.RouteServerEndpointID, p.RouteServerEndpointID)
		assert.Equal(t, rs.RouteServerID, p.RouteServerID)
		assert.Equal(t, "10.0.0.5", p.PeerAddress)
		assert.Equal(t, int64(65001), p.BgpPeerAsn)
		assert.Equal(t, "bfd", p.BgpPeerLivenessDetectionMode)
		assert.Equal(t, "available", p.State)
		peerID = p.RouteServerPeerID
	})

	t.Run("create peer defaults liveness detection", func(t *testing.T) { //nolint:paralleltest // existing issue.
		p, err := b.CreateRouteServerPeer(ep.RouteServerEndpointID, "10.0.0.6", 65002, "")
		require.NoError(t, err)
		assert.Equal(t, "bfd", p.BgpPeerLivenessDetectionMode)
	})

	t.Run("describe returns created peer", func(t *testing.T) { //nolint:paralleltest // existing issue.
		peers := b.DescribeRouteServerPeers([]string{peerID})
		require.Len(t, peers, 1)
		assert.Equal(t, peerID, peers[0].RouteServerPeerID)
	})

	t.Run("describe all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		peers := b.DescribeRouteServerPeers(nil)
		assert.NotEmpty(t, peers)
	})

	t.Run("delete peer", func(t *testing.T) { //nolint:paralleltest // existing issue.
		p, err := b.DeleteRouteServerPeer(peerID)
		require.NoError(t, err)
		assert.Equal(t, "deleting", p.State)

		peers := b.DescribeRouteServerPeers([]string{peerID})
		assert.Empty(t, peers)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteRouteServerPeer("rsp-nonexistent")
		require.Error(t, err)
	})
}

func TestRouteServer_Association(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rs, setupErr := b.CreateRouteServer(65000, "enabled", 60, false)
	require.NoError(t, setupErr)

	t.Run("associate requires vpc id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.AssociateRouteServer(rs.RouteServerID, "")
		require.Error(t, err)
	})

	t.Run("associate with unknown route server errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.AssociateRouteServer("rs-nonexistent", "vpc-default")
		require.Error(t, err)
	})

	t.Run("associate with unknown vpc returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.AssociateRouteServer(rs.RouteServerID, "vpc-nonexistent")
		require.Error(t, err)
	})

	t.Run("associate route server", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assoc, err := b.AssociateRouteServer(rs.RouteServerID, "vpc-default")
		require.NoError(t, err)
		assert.Equal(t, rs.RouteServerID, assoc.RouteServerID)
		assert.Equal(t, "vpc-default", assoc.VpcID)
		assert.Equal(t, "associated", assoc.State)
	})

	t.Run("get associations", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assocs := b.GetRouteServerAssociations(rs.RouteServerID)
		require.Len(t, assocs, 1)
		assert.Equal(t, "vpc-default", assocs[0].VpcID)
	})

	t.Run("disassociate route server", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assoc, err := b.DisassociateRouteServer(rs.RouteServerID, "vpc-default")
		require.NoError(t, err)
		assert.Equal(t, "disassociating", assoc.State)

		assocs := b.GetRouteServerAssociations(rs.RouteServerID)
		assert.Empty(t, assocs)
	})

	t.Run("disassociate non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DisassociateRouteServer(rs.RouteServerID, "vpc-default")
		require.Error(t, err)
	})
}

func TestRouteServer_Propagation(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rs, setupErr := b.CreateRouteServer(65000, "enabled", 60, false)
	require.NoError(t, setupErr)
	rt, setupErr := b.CreateRouteTable("vpc-default")
	require.NoError(t, setupErr)

	t.Run("enable requires route table id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.EnableRouteServerPropagation(rs.RouteServerID, "")
		require.Error(t, err)
	})

	t.Run("enable with unknown route server errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.EnableRouteServerPropagation("rs-nonexistent", rt.ID)
		require.Error(t, err)
	})

	t.Run("enable with unknown route table returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.EnableRouteServerPropagation(rs.RouteServerID, "rtb-nonexistent")
		require.Error(t, err)
	})

	t.Run("enable propagation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		prop, err := b.EnableRouteServerPropagation(rs.RouteServerID, rt.ID)
		require.NoError(t, err)
		assert.Equal(t, rs.RouteServerID, prop.RouteServerID)
		assert.Equal(t, rt.ID, prop.RouteTableID)
		assert.Equal(t, "enabled", prop.State)
	})

	t.Run("get propagations", func(t *testing.T) { //nolint:paralleltest // existing issue.
		props := b.GetRouteServerPropagations(rs.RouteServerID)
		require.Len(t, props, 1)
		assert.Equal(t, rt.ID, props[0].RouteTableID)
	})

	t.Run("disable propagation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		prop, err := b.DisableRouteServerPropagation(rs.RouteServerID, rt.ID)
		require.NoError(t, err)
		assert.Equal(t, "disabling", prop.State)

		props := b.GetRouteServerPropagations(rs.RouteServerID)
		assert.Empty(t, props)
	})

	t.Run("disable non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DisableRouteServerPropagation(rs.RouteServerID, rt.ID)
		require.Error(t, err)
	})
}

func TestRouteServer_RoutingDatabase(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rs, setupErr := b.CreateRouteServer(65000, "enabled", 60, false)
	require.NoError(t, setupErr)

	t.Run("returns empty routing database when fresh", func(t *testing.T) { //nolint:paralleltest // existing issue.
		routes, err := b.GetRouteServerRoutingDatabase(rs.RouteServerID)
		require.NoError(t, err)
		assert.Empty(t, routes)
	})

	t.Run("unknown route server returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.GetRouteServerRoutingDatabase("rs-nonexistent")
		require.Error(t, err)
	})
}
