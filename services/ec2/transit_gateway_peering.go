package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// CreateTransitGatewayPeeringAttachment creates a TGW peering attachment.
func (b *InMemoryBackend) CreateTransitGatewayPeeringAttachment(
	transitGatewayID, peerTransitGatewayID, _ string,
) (*TransitGatewayPeeringAttachment, error) {
	if transitGatewayID == "" || peerTransitGatewayID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayId and PeerTransitGatewayId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayPeeringAttachment")
	defer b.mu.Unlock()

	id := "tgw-attach-" + uuid.New().String()[:8]
	att := &TransitGatewayPeeringAttachment{
		TransitGatewayAttachmentID: id,
		RequesterTransitGatewayID:  transitGatewayID,
		AccepterTransitGatewayID:   peerTransitGatewayID,
		State:                      "pendingAcceptance",
	}
	b.tgwPeeringAttachments.Put(att)

	return att, nil
}

// DeleteTransitGatewayPeeringAttachment removes a TGW peering attachment.
func (b *InMemoryBackend) DeleteTransitGatewayPeeringAttachment(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayPeeringAttachment")
	defer b.mu.Unlock()

	if _, ok := b.tgwPeeringAttachments.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, id)
	}
	b.tgwPeeringAttachments.Delete(id)
	delete(b.tags, id)

	return nil
}

// DescribeTransitGatewayPeeringAttachments returns TGW peering attachments.
func (b *InMemoryBackend) DescribeTransitGatewayPeeringAttachments(
	ids []string,
) []*TransitGatewayPeeringAttachment {
	b.mu.RLock("DescribeTransitGatewayPeeringAttachments")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*TransitGatewayPeeringAttachment
	for _, att := range b.tgwPeeringAttachments.All() {
		if len(filter) > 0 && !filter[att.TransitGatewayAttachmentID] {
			continue
		}
		cp := *att
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// ---- TransitGatewayConnect ----

// CreateTransitGatewayConnect creates a TGW connect attachment.
func (b *InMemoryBackend) CreateTransitGatewayConnect(
	transportAttachmentID, transitGatewayID string,
) (*TransitGatewayConnect, error) {
	if transportAttachmentID == "" || transitGatewayID == "" {
		return nil, fmt.Errorf(
			"%w: TransportTransitGatewayAttachmentId and TransitGatewayId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayConnect")
	defer b.mu.Unlock()

	id := "tgw-attach-" + uuid.New().String()[:8]
	conn := &TransitGatewayConnect{
		TransitGatewayAttachmentID:          id,
		TransportTransitGatewayAttachmentID: transportAttachmentID,
		TransitGatewayID:                    transitGatewayID,
		State:                               stateAvailable,
	}
	b.tgwConnects.Put(conn)

	return conn, nil
}

// DeleteTransitGatewayConnect removes a TGW connect attachment.
func (b *InMemoryBackend) DeleteTransitGatewayConnect(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayConnect")
	defer b.mu.Unlock()

	if _, ok := b.tgwConnects.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTransitGatewayConnectNotFound, id)
	}
	b.tgwConnects.Delete(id)
	delete(b.tags, id)

	return nil
}

// DescribeTransitGatewayConnects returns TGW connect attachments.
func (b *InMemoryBackend) DescribeTransitGatewayConnects(ids []string) []*TransitGatewayConnect {
	b.mu.RLock("DescribeTransitGatewayConnects")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*TransitGatewayConnect
	for _, conn := range b.tgwConnects.All() {
		if len(filter) > 0 && !filter[conn.TransitGatewayAttachmentID] {
			continue
		}
		cp := *conn
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayAttachmentID < out[j].TransitGatewayAttachmentID
	})

	return out
}

// CreateTransitGatewayConnectPeer creates a TGW connect peer.
func (b *InMemoryBackend) CreateTransitGatewayConnectPeer(
	connectAttachmentID, peerAddress string,
	insideCidrBlocks []string,
) (*TransitGatewayConnectPeer, error) {
	if connectAttachmentID == "" || peerAddress == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayAttachmentId and PeerAddress are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	if _, ok := b.tgwConnects.Get(connectAttachmentID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayConnectNotFound, connectAttachmentID)
	}

	id := "tgw-connect-peer-" + uuid.New().String()[:8]
	peer := &TransitGatewayConnectPeer{
		TransitGatewayConnectPeerID: id,
		TransitGatewayAttachmentID:  connectAttachmentID,
		State:                       stateAvailable,
		InsideCidrBlocks:            insideCidrBlocks,
		PeerAddress:                 peerAddress,
	}
	b.tgwConnectPeers.Put(peer)

	return peer, nil
}

// DeleteTransitGatewayConnectPeer removes a TGW connect peer.
func (b *InMemoryBackend) DeleteTransitGatewayConnectPeer(id string) error {
	if id == "" {
		return fmt.Errorf("%w: TransitGatewayConnectPeerId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	if _, ok := b.tgwConnectPeers.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrTransitGatewayConnectPeerNotFound, id)
	}
	b.tgwConnectPeers.Delete(id)
	delete(b.tags, id)

	return nil
}

// DescribeTransitGatewayConnectPeers returns TGW connect peers.
func (b *InMemoryBackend) DescribeTransitGatewayConnectPeers(
	ids []string,
) []*TransitGatewayConnectPeer {
	b.mu.RLock("DescribeTransitGatewayConnectPeers")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*TransitGatewayConnectPeer
	for _, peer := range b.tgwConnectPeers.All() {
		if len(filter) > 0 && !filter[peer.TransitGatewayConnectPeerID] {
			continue
		}
		cp := *peer
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TransitGatewayConnectPeerID < out[j].TransitGatewayConnectPeerID
	})

	return out
}

// ---- TransitGatewayPrefixListReference ----

// CreateTransitGatewayPrefixListReference creates a TGW prefix list reference.
func (b *InMemoryBackend) CreateTransitGatewayPrefixListReference(
	routeTableID, prefixListID string,
	blackhole bool,
) (*TransitGatewayPrefixListReference, error) {
	if routeTableID == "" || prefixListID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayRouteTableId and PrefixListId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayPrefixListReference")
	defer b.mu.Unlock()

	ref := &TransitGatewayPrefixListReference{
		PrefixListID:               prefixListID,
		TransitGatewayRouteTableID: routeTableID,
		State:                      stateAvailable,
		Blackhole:                  blackhole,
	}
	b.tgwPrefixListRefs.Put(ref)

	return ref, nil
}

// DeleteTransitGatewayPrefixListReference removes a TGW prefix list reference.
func (b *InMemoryBackend) DeleteTransitGatewayPrefixListReference(
	routeTableID, prefixListID string,
) error {
	if routeTableID == "" || prefixListID == "" {
		return fmt.Errorf(
			"%w: TransitGatewayRouteTableId and PrefixListId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteTransitGatewayPrefixListReference")
	defer b.mu.Unlock()

	key := routeTableID + "/" + prefixListID
	if _, ok := b.tgwPrefixListRefs.Get(key); !ok {
		return fmt.Errorf("%w: %s/%s", ErrTGWPrefixListRefNotFound, routeTableID, prefixListID)
	}
	b.tgwPrefixListRefs.Delete(key)

	return nil
}

// GetTransitGatewayPrefixListReferences returns TGW prefix list references for a route table.
func (b *InMemoryBackend) GetTransitGatewayPrefixListReferences(
	routeTableID string,
) ([]*TransitGatewayPrefixListReference, error) {
	if routeTableID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayRouteTableId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetTransitGatewayPrefixListReferences")
	defer b.mu.RUnlock()

	var out []*TransitGatewayPrefixListReference
	for _, ref := range b.tgwPrefixListRefs.All() {
		if ref.TransitGatewayRouteTableID == routeTableID {
			cp := *ref
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PrefixListID < out[j].PrefixListID })

	return out, nil
}

// ModifyTransitGatewayPrefixListReference updates the blackhole flag and/or
// target attachment of an existing TGW prefix list reference.
func (b *InMemoryBackend) ModifyTransitGatewayPrefixListReference(
	routeTableID, prefixListID, attachmentID string,
	blackhole bool,
) (*TransitGatewayPrefixListReference, error) {
	if routeTableID == "" || prefixListID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayRouteTableId and PrefixListId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyTransitGatewayPrefixListReference")
	defer b.mu.Unlock()

	key := routeTableID + "/" + prefixListID
	ref, ok := b.tgwPrefixListRefs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrTGWPrefixListRefNotFound, routeTableID, prefixListID)
	}

	ref.Blackhole = blackhole
	if attachmentID != "" {
		ref.TransitGatewayAttachmentID = attachmentID
	}

	cp := *ref

	return &cp, nil
}

// ---- VerifiedAccess ----
