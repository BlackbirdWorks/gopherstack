package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- ManagedPrefixList ----

func TestBatch4_ManagedPrefixList(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var plID string

	t.Run("create prefix list", func(t *testing.T) { //nolint:paralleltest // existing issue.
		pl, err := b.CreateManagedPrefixList("my-list", "IPv4", 10)
		require.NoError(t, err)
		assert.NotEmpty(t, pl.PrefixListID)
		assert.Equal(t, "my-list", pl.PrefixListName)
		assert.Equal(t, "IPv4", pl.AddressFamily)
		assert.Equal(t, "create-complete", pl.State)
		assert.Equal(t, int64(1), pl.Version)
		plID = pl.PrefixListID
	})

	t.Run("describe returns created list", func(t *testing.T) { //nolint:paralleltest // existing issue.
		lists := b.DescribeManagedPrefixLists([]string{plID})
		require.Len(t, lists, 1)
		assert.Equal(t, "my-list", lists[0].PrefixListName)
	})

	t.Run("modify add entries", func(t *testing.T) { //nolint:paralleltest // existing issue.
		err := b.ModifyManagedPrefixList(plID, []ec2.PrefixListEntry{
			{Cidr: "10.0.0.0/8", Description: "private"},
		}, nil)
		require.NoError(t, err)
	})

	t.Run("get entries returns added", func(t *testing.T) { //nolint:paralleltest // existing issue.
		entries, err := b.GetManagedPrefixListEntries(plID)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "10.0.0.0/8", entries[0].Cidr)
	})

	t.Run("restore version", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.RestoreManagedPrefixListVersion(plID, 1))
		lists := b.DescribeManagedPrefixLists([]string{plID})
		require.Len(t, lists, 1)
		assert.Equal(t, "restore-complete", lists[0].State)
	})

	t.Run("delete prefix list", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteManagedPrefixList(plID))
		lists := b.DescribeManagedPrefixLists(nil)
		assert.Empty(t, lists)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.DeleteManagedPrefixList("pl-nonexistent"))
	})

	t.Run("create with empty name returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateManagedPrefixList("", "IPv4", 10)
		require.Error(t, err)
	})
}

// ---- ClientVpnEndpoint ----

func TestBatch4_ClientVpnEndpoint(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var epID string

	t.Run("create endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.CreateClientVpnEndpoint("10.0.0.0/22", "test vpn", nil)
		require.NoError(t, err)
		assert.NotEmpty(t, ep.ClientVpnEndpointID)
		assert.Equal(t, "available", ep.Status)
		assert.Equal(t, "10.0.0.0/22", ep.ClientCidrBlock)
		epID = ep.ClientVpnEndpointID
	})

	t.Run("describe returns endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		eps := b.DescribeClientVpnEndpoints([]string{epID})
		require.Len(t, eps, 1)
		assert.Equal(t, "10.0.0.0/22", eps[0].ClientCidrBlock)
	})

	t.Run("associate target network", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assocID, assocErr := b.AssociateClientVpnTargetNetwork(epID, "subnet-default")
		require.NoError(t, assocErr)
		assert.NotEmpty(t, assocID)
		networks, err := b.DescribeClientVpnTargetNetworks(epID)
		require.NoError(t, err)
		require.Len(t, networks, 1)
		assert.Equal(t, "subnet-default", networks[0].SubnetID)
		assert.Equal(t, assocID, networks[0].AssociationID)
	})

	t.Run("disassociate target network", func(t *testing.T) { //nolint:paralleltest // existing issue.
		// get the association ID first
		networks, listErr := b.DescribeClientVpnTargetNetworks(epID)
		require.NoError(t, listErr)
		require.Len(t, networks, 1)
		require.NoError(t, b.DisassociateClientVpnTargetNetwork(epID, networks[0].AssociationID))
		networks, err := b.DescribeClientVpnTargetNetworks(epID)
		require.NoError(t, err)
		assert.Empty(t, networks)
	})

	t.Run("create route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.CreateClientVpnRoute(epID, "0.0.0.0/0", "default route"))
		routes, err := b.DescribeClientVpnRoutes(epID)
		require.NoError(t, err)
		require.Len(t, routes, 1)
		assert.Equal(t, "0.0.0.0/0", routes[0].DestinationCidr)
	})

	t.Run("delete route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteClientVpnRoute(epID, "0.0.0.0/0"))
		routes, err := b.DescribeClientVpnRoutes(epID)
		require.NoError(t, err)
		assert.Empty(t, routes)
	})

	t.Run("authorize ingress", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.AuthorizeClientVpnIngress(epID, "10.0.0.0/8", "private"))
		rules, err := b.DescribeClientVpnAuthorizationRules(epID)
		require.NoError(t, err)
		require.Len(t, rules, 1)
		assert.Equal(t, "10.0.0.0/8", rules[0].Cidr)
	})

	t.Run("revoke ingress", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.RevokeClientVpnIngress(epID, "10.0.0.0/8"))
		rules, err := b.DescribeClientVpnAuthorizationRules(epID)
		require.NoError(t, err)
		assert.Empty(t, rules)
	})

	t.Run("modify endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyClientVpnEndpoint(epID, "updated vpn", nil))
		eps := b.DescribeClientVpnEndpoints([]string{epID})
		require.Len(t, eps, 1)
		assert.Equal(t, "updated vpn", eps[0].Description)
	})

	t.Run("describe connections returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		conns, err := b.DescribeClientVpnConnections(epID)
		require.NoError(t, err)
		assert.Empty(t, conns)
	})

	t.Run("terminate connections", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.TerminateClientVpnConnections(epID))
	})

	t.Run("apply security groups", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ApplySecurityGroupsToClientVpnTargetNetwork(epID, []string{"sg-default"}))
	})

	t.Run("delete endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteClientVpnEndpoint(epID))
		eps := b.DescribeClientVpnEndpoints(nil)
		assert.Empty(t, eps)
	})

	t.Run("create with empty cidr returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateClientVpnEndpoint("", "test", nil)
		require.Error(t, err)
	})
}

// ---- TransitGatewayPeeringAttachment ----.
func TestBatch4_TGWPeeringAttachment(t *testing.T) { //nolint:paralleltest // existing issue.
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
func TestBatch4_TGWConnect(t *testing.T) { //nolint:paralleltest // existing issue.
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
func TestBatch4_TGWPrefixListReference(t *testing.T) { //nolint:paralleltest // existing issue.
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
func TestBatch4_VerifiedAccess(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var instanceID, groupID, endpointID, trustProviderID string

	t.Run("create instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		inst, err := b.CreateVerifiedAccessInstance("test instance")
		require.NoError(t, err)
		assert.NotEmpty(t, inst.VerifiedAccessInstanceID)
		assert.Equal(t, "active", inst.Status)
		instanceID = inst.VerifiedAccessInstanceID
	})

	t.Run("describe instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		instances := b.DescribeVerifiedAccessInstances([]string{instanceID})
		require.Len(t, instances, 1)
		assert.Equal(t, "test instance", instances[0].Description)
	})

	t.Run("create trust provider", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tp, err := b.CreateVerifiedAccessTrustProvider("user", "test provider")
		require.NoError(t, err)
		assert.NotEmpty(t, tp.VerifiedAccessTrustProviderID)
		trustProviderID = tp.VerifiedAccessTrustProviderID
	})

	t.Run("attach trust provider to instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.AttachVerifiedAccessTrustProvider(instanceID, trustProviderID))
		// double-attach is idempotent
		require.NoError(t, b.AttachVerifiedAccessTrustProvider(instanceID, trustProviderID))
	})

	t.Run("detach trust provider from instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DetachVerifiedAccessTrustProvider(instanceID, trustProviderID))
	})

	t.Run("create group", func(t *testing.T) { //nolint:paralleltest // existing issue.
		grp, err := b.CreateVerifiedAccessGroup(instanceID, "test group")
		require.NoError(t, err)
		assert.NotEmpty(t, grp.VerifiedAccessGroupID)
		groupID = grp.VerifiedAccessGroupID
	})

	t.Run("describe groups", func(t *testing.T) { //nolint:paralleltest // existing issue.
		groups := b.DescribeVerifiedAccessGroups([]string{groupID})
		require.Len(t, groups, 1)
		assert.Equal(t, instanceID, groups[0].VerifiedAccessInstanceID)
	})

	t.Run("create endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.CreateVerifiedAccessEndpoint(groupID, "load-balancer", "test endpoint")
		require.NoError(t, err)
		assert.NotEmpty(t, ep.VerifiedAccessEndpointID)
		endpointID = ep.VerifiedAccessEndpointID
	})

	t.Run("describe endpoints", func(t *testing.T) { //nolint:paralleltest // existing issue.
		eps := b.DescribeVerifiedAccessEndpoints([]string{endpointID})
		require.Len(t, eps, 1)
		assert.Equal(t, groupID, eps[0].VerifiedAccessGroupID)
	})

	t.Run("modify endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyVerifiedAccessEndpoint(endpointID, "updated endpoint"))
		eps := b.DescribeVerifiedAccessEndpoints([]string{endpointID})
		require.Len(t, eps, 1)
		assert.Equal(t, "updated endpoint", eps[0].Description)
	})

	t.Run("describe trust providers", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tps := b.DescribeVerifiedAccessTrustProviders([]string{trustProviderID})
		require.Len(t, tps, 1)
		assert.Equal(t, "user", tps[0].TrustProviderType)
	})

	t.Run("delete endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessEndpoint(endpointID))
		eps := b.DescribeVerifiedAccessEndpoints(nil)
		assert.Empty(t, eps)
	})

	t.Run("delete group", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessGroup(groupID))
		groups := b.DescribeVerifiedAccessGroups(nil)
		assert.Empty(t, groups)
	})

	t.Run("delete trust provider", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessTrustProvider(trustProviderID))
		tps := b.DescribeVerifiedAccessTrustProviders(nil)
		assert.Empty(t, tps)
	})

	t.Run("delete instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVerifiedAccessInstance(instanceID))
		instances := b.DescribeVerifiedAccessInstances(nil)
		assert.Empty(t, instances)
	})

	t.Run( //nolint:paralleltest // existing issue.
		"create group for non-existent instance still works",
		func(t *testing.T) {
			// Group creation doesn't validate instance existence in mock
			_, err := b.CreateVerifiedAccessGroup("vai-nonexistent", "group")
			require.NoError(t, err)
		},
	)

	t.Run("attach to non-existent instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tp2, err := b.CreateVerifiedAccessTrustProvider("device", "desc")
		require.NoError(t, err)
		require.Error(t, b.AttachVerifiedAccessTrustProvider("vai-nonexistent", tp2.VerifiedAccessTrustProviderID))
	})
}
