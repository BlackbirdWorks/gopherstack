package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- TransitGatewayPeeringAttachment ----.
func TestTGWPeeringAttachment(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var attID string

	t.Run("create peering attachment", func(t *testing.T) { //nolint:paralleltest // existing issue.
		att, err := b.CreateTransitGatewayPeeringAttachment("tgw-111", "tgw-222", "us-west-2")
		require.NoError(t, err)
		assert.NotEmpty(t, att.TransitGatewayAttachmentID)
		assert.Equal(t, "pendingAcceptance", att.State)
		attID = att.TransitGatewayAttachmentID
	})

	t.Run("describe returns attachment", func(t *testing.T) { //nolint:paralleltest // existing issue.
		atts := b.DescribeTransitGatewayPeeringAttachments([]string{attID})
		require.Len(t, atts, 1)
		assert.Equal(t, "tgw-111", atts[0].RequesterTransitGatewayID)
	})

	t.Run("delete attachment", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTransitGatewayPeeringAttachment(attID))
		atts := b.DescribeTransitGatewayPeeringAttachments(nil)
		assert.Empty(t, atts)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteTransitGatewayPeeringAttachment("tgw-attach-nonexistent"))
	})
}

// ---- TransitGatewayConnect ----.

// ---- TransitGatewayConnect ----.
func TestTGWConnect(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var connectID, peerID string

	t.Run("create connect", func(t *testing.T) { //nolint:paralleltest // existing issue.
		conn, err := b.CreateTransitGatewayConnect("tgw-attach-transport", "tgw-111")
		require.NoError(t, err)
		assert.NotEmpty(t, conn.TransitGatewayAttachmentID)
		assert.Equal(t, "available", conn.State)
		connectID = conn.TransitGatewayAttachmentID
	})

	t.Run("describe connects", func(t *testing.T) { //nolint:paralleltest // existing issue.
		conns := b.DescribeTransitGatewayConnects([]string{connectID})
		require.Len(t, conns, 1)
		assert.Equal(t, "tgw-111", conns[0].TransitGatewayID)
	})

	t.Run("create connect peer", func(t *testing.T) { //nolint:paralleltest // existing issue.
		peer, err := b.CreateTransitGatewayConnectPeer(connectID, "1.2.3.4", []string{"169.254.6.0/29"})
		require.NoError(t, err)
		assert.NotEmpty(t, peer.TransitGatewayConnectPeerID)
		assert.Equal(t, "1.2.3.4", peer.PeerAddress)
		peerID = peer.TransitGatewayConnectPeerID
	})

	t.Run("describe connect peers", func(t *testing.T) { //nolint:paralleltest // existing issue.
		peers := b.DescribeTransitGatewayConnectPeers([]string{peerID})
		require.Len(t, peers, 1)
		assert.Equal(t, connectID, peers[0].TransitGatewayAttachmentID)
	})

	t.Run("delete connect peer", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTransitGatewayConnectPeer(peerID))
		peers := b.DescribeTransitGatewayConnectPeers(nil)
		assert.Empty(t, peers)
	})

	t.Run("delete connect", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTransitGatewayConnect(connectID))
		conns := b.DescribeTransitGatewayConnects(nil)
		assert.Empty(t, conns)
	})

	t.Run( //nolint:paralleltest // existing issue.
		"create peer for non-existent connect returns error",
		func(t *testing.T) {
			_, err := b.CreateTransitGatewayConnectPeer("nonexistent", "1.2.3.4", nil)
			require.Error(t, err)
		},
	)
}

// ---- TransitGatewayPrefixListReference ----.

// ---- TransitGatewayPrefixListReference ----.
func TestTGWPrefixListReference(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("create reference", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ref, err := b.CreateTransitGatewayPrefixListReference("tgw-rtb-111", "pl-abc123", false)
		require.NoError(t, err)
		assert.Equal(t, "pl-abc123", ref.PrefixListID)
		assert.Equal(t, "available", ref.State)
		assert.False(t, ref.Blackhole)
	})

	t.Run("get references for route table", func(t *testing.T) { //nolint:paralleltest // existing issue.
		refs, err := b.GetTransitGatewayPrefixListReferences("tgw-rtb-111")
		require.NoError(t, err)
		require.Len(t, refs, 1)
		assert.Equal(t, "pl-abc123", refs[0].PrefixListID)
	})

	t.Run("delete reference", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteTransitGatewayPrefixListReference("tgw-rtb-111", "pl-abc123"))
		refs, err := b.GetTransitGatewayPrefixListReferences("tgw-rtb-111")
		require.NoError(t, err)
		assert.Empty(t, refs)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteTransitGatewayPrefixListReference("tgw-rtb-111", "pl-nonexistent"))
	})
}

// ---- VerifiedAccess ----.

// TestTGW_PeeringAttachmentCRUD verifies TGW peering full CRUD cycle.
func TestTGW_PeeringAttachmentCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	att, err := b.CreateTransitGatewayPeeringAttachment("tgw-src", "tgw-dst", "us-west-2")
	require.NoError(t, err)
	assert.NotEmpty(t, att.TransitGatewayAttachmentID)
	assert.Contains(t, att.TransitGatewayAttachmentID, "tgw-attach-")
	assert.Equal(t, "tgw-src", att.RequesterTransitGatewayID)
	assert.Equal(t, "tgw-dst", att.AccepterTransitGatewayID)

	atts := b.DescribeTransitGatewayPeeringAttachments([]string{att.TransitGatewayAttachmentID})
	require.Len(t, atts, 1)
	assert.Equal(t, att.TransitGatewayAttachmentID, atts[0].TransitGatewayAttachmentID)

	require.NoError(t, b.DeleteTransitGatewayPeeringAttachment(att.TransitGatewayAttachmentID))
	atts2 := b.DescribeTransitGatewayPeeringAttachments(nil)
	assert.Empty(t, atts2)
}

// TestTGW_PeeringAttachment_ViaHandler tests handler-level response shape.

// TestTGW_PeeringAttachment_ViaHandler tests handler-level response shape.
func TestTGW_PeeringAttachment_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":               {"CreateTransitGatewayPeeringAttachment"},
		"TransitGatewayId":     {"tgw-111"},
		"PeerTransitGatewayId": {"tgw-222"},
		"PeerRegion":           {"us-west-2"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<transitGatewayAttachmentId>tgw-attach-")
	assert.Contains(t, resp, "<CreateTransitGatewayPeeringAttachmentResponse")
}

// TestTGW_ConnectCRUD verifies TGW connect CRUD cycle.

// TestTGW_ConnectCRUD verifies TGW connect CRUD cycle.
func TestTGW_ConnectCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	conn, err := b.CreateTransitGatewayConnect("tgw-attach-transport", "tgw-src")
	require.NoError(t, err)
	assert.NotEmpty(t, conn.TransitGatewayAttachmentID)

	// add a connect peer
	peer, err := b.CreateTransitGatewayConnectPeer(
		conn.TransitGatewayAttachmentID, "1.2.3.4", []string{"169.254.6.0/29"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, peer.TransitGatewayConnectPeerID)
	assert.Contains(t, peer.TransitGatewayConnectPeerID, "tgw-connect-peer-")

	peers := b.DescribeTransitGatewayConnectPeers([]string{peer.TransitGatewayConnectPeerID})
	require.Len(t, peers, 1)
	assert.Equal(t, "1.2.3.4", peers[0].PeerAddress)

	require.NoError(t, b.DeleteTransitGatewayConnectPeer(peer.TransitGatewayConnectPeerID))
	require.NoError(t, b.DeleteTransitGatewayConnect(conn.TransitGatewayAttachmentID))
}

// TestTGW_PrefixListRefCRUD verifies TGW prefix list reference CRUD.

// TestTGW_PrefixListRefCRUD verifies TGW prefix list reference CRUD.
func TestTGW_PrefixListRefCRUD(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	ref, err := b.CreateTransitGatewayPrefixListReference("tgw-rtb-111", "pl-abc123", false)
	require.NoError(t, err)
	assert.Equal(t, "pl-abc123", ref.PrefixListID)
	assert.Equal(t, "tgw-rtb-111", ref.TransitGatewayRouteTableID)

	refs, err := b.GetTransitGatewayPrefixListReferences("tgw-rtb-111")
	require.NoError(t, err)
	require.Len(t, refs, 1)

	require.NoError(t, b.DeleteTransitGatewayPrefixListReference("tgw-rtb-111", "pl-abc123"))
	refs2, err := b.GetTransitGatewayPrefixListReferences("tgw-rtb-111")
	require.NoError(t, err)
	assert.Empty(t, refs2)
}

// ============================================================================
// ManagedPrefixList accuracy
// ============================================================================

// TestManagedPrefixList_FullCycle tests full CRUD and entry management.

// TestTGWPeeringAttachmentCreationTime verifies CreationTime is set.
func TestTGWPeeringAttachmentCreationTime(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddTGWPeeringAttachmentInternal(&ec2.TransitGatewayPeeringAttachment{
		TransitGatewayAttachmentID: "tgw-attach-1",
	})

	res, err := b.AcceptTransitGatewayPeeringAttachment("tgw-attach-1")
	require.NoError(t, err)
	assert.False(t, res.CreationTime.IsZero())
}
