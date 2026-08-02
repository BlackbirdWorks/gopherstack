package networkmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file implements PARITY.md family K: the Cloud WAN-side Connect Peer
// lifecycle (a BGP/GRE peer terminating a Connect attachment). ConnectPeer
// is distinct from ConnectPeerAssociation (family J, associations.go),
// which binds an already-created ConnectPeer here to a Global-Networks
// Device/Link.

// CreateConnectPeer creates a new Connect peer terminating connectAttachmentID
// (which must already exist and be a CONNECT attachment). State starts
// CREATING (ConnectPeerState has no PENDING value, unlike every other
// resource in this service), transitioning to AVAILABLE.
func (b *InMemoryBackend) CreateConnectPeer(
	connectAttachmentID, peerAddress string,
	bgp *BgpOptions,
	coreNetworkAddress, subnetArn string,
	insideCidrs []string,
	tagMap map[string]string,
) (*ConnectPeer, error) {
	b.mu.Lock("CreateConnectPeer")
	defer b.mu.Unlock()

	att, ok := b.attachments.Get(connectAttachmentID)
	if !ok || att.AttachmentType != attachmentTypeConnect {
		return nil, notFoundError(resourceAttachment, connectAttachmentID)
	}

	if peerAddress == "" {
		return nil, validationError("PeerAddress is required")
	}

	protocol := tunnelProtocolNoEncap
	if bgp != nil {
		protocol = tunnelProtocolGre
	}

	id := newConnectPeerID()
	c := &ConnectPeer{
		ConnectPeerID:       id,
		ConnectAttachmentID: connectAttachmentID,
		CoreNetworkID:       att.CoreNetworkID,
		CreatedAt:           nowUTC(),
		EdgeLocation:        att.EdgeLocation,
		State:               connectPeerStateCreating,
		SubnetArn:           subnetArn,
		Configuration: &ConnectPeerConfiguration{
			CoreNetworkAddress: coreNetworkAddress,
			InsideCidrBlocks:   append([]string(nil), insideCidrs...),
			PeerAddress:        peerAddress,
			Protocol:           protocol,
		},
		Tags: tags.FromMap("networkmanager.connectpeer."+id+".tags", tagMap),
	}

	if bgp != nil {
		c.Configuration.BgpConfigurations = []ConnectPeerBgpConfiguration{
			{PeerAddress: peerAddress, PeerAsn: bgp.PeerAsn, CoreNetworkAddress: coreNetworkAddress},
		}
	}

	b.connectPeers.Put(c)

	scheduleAdvance(b, "ConnectPeerAvailable", b.connectPeers, id,
		func(v *ConnectPeer) *string { return &v.State }, connectPeerStateCreating, connectPeerStateAvailable)

	return c.clone(), nil
}

func (b *InMemoryBackend) DeleteConnectPeer(id string) (*ConnectPeer, error) {
	b.mu.Lock("DeleteConnectPeer")
	defer b.mu.Unlock()

	c, ok := b.connectPeers.Get(id)
	if !ok {
		return nil, notFoundError(resourceConnectPeer, id)
	}

	c.State = connectPeerStateDeleting
	scheduleRemoval(b, "ConnectPeerDeleted", b.connectPeers, id)

	return c.clone(), nil
}

func (b *InMemoryBackend) GetConnectPeer(id string) (*ConnectPeer, error) {
	b.mu.RLock("GetConnectPeer")
	defer b.mu.RUnlock()

	c, ok := b.connectPeers.Get(id)
	if !ok {
		return nil, notFoundError(resourceConnectPeer, id)
	}

	return c.clone(), nil
}

func (b *InMemoryBackend) ListConnectPeers(
	connectAttachmentID, coreNetworkID, token string, limit int,
) (page.Page[*ConnectPeer], error) {
	b.mu.RLock("ListConnectPeers")
	defer b.mu.RUnlock()

	all := b.connectPeers.Snapshot()
	all = filterOptional(all, connectAttachmentID, func(c *ConnectPeer) string { return c.ConnectAttachmentID })
	all = filterOptional(all, coreNetworkID, func(c *ConnectPeer) string { return c.CoreNetworkID })

	p := sortAndPage(all, func(c *ConnectPeer) string { return c.ConnectPeerID }, (*ConnectPeer).clone, token, limit)

	return p, nil
}
