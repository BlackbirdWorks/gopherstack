package ec2

import (
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CreateTransitGatewayPeeringAttachment creates a TGW peering attachment.
func (b *InMemoryBackend) CreateTransitGatewayPeeringAttachment(
	transitGatewayID, peerTransitGatewayID, peerAccountID, peerRegion string,
) (*TransitGatewayPeeringAttachment, error) {
	if transitGatewayID == "" || peerTransitGatewayID == "" || peerAccountID == "" || peerRegion == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayId, PeerTransitGatewayId, PeerAccountId, and PeerRegion are required",
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
		RequesterOwnerID:           b.AccountID,
		RequesterRegion:            b.Region,
		AccepterOwnerID:            peerAccountID,
		AccepterRegion:             peerRegion,
		State:                      "pendingAcceptance",
	}
	b.tgwPeeringAttachments.Put(att)

	return att, nil
}

// DeleteTransitGatewayPeeringAttachment removes a TGW peering attachment.
// DeleteTransitGatewayPeeringAttachment removes a TGW peering attachment,
// keeping a tombstone in state "deleted" so a subsequent by-ID Describe
// still finds it (real AWS keeps a deleted attachment describable for a
// period; terraform-provider-aws's delete waiter polls by ID and treats a
// NotFound response as a fatal error instead of "done" -- see
// nat_gateways.go's DeleteNatGateway for the same pattern).
func (b *InMemoryBackend) DeleteTransitGatewayPeeringAttachment(id string) (*TransitGatewayPeeringAttachment, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayPeeringAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwPeeringAttachments.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, id)
	}
	cp := *att
	b.tgwPeeringAttachments.Delete(id)
	delete(b.tags, id)

	tombstoned := cp
	tombstoned.State = tgwRouteStateDeleted
	pruneExpiredTombstones(b.tgwPeeringAttachmentTombstones, time.Now())
	b.tgwPeeringAttachmentTombstones[id] = tombstone[TransitGatewayPeeringAttachment]{
		value:     &tombstoned,
		deletedAt: time.Now(),
	}

	return &cp, nil
}

// DescribeTransitGatewayPeeringAttachments returns TGW peering attachments.
// When ids are provided, a tombstone is returned for any of them that was
// recently deleted (see DeleteTransitGatewayPeeringAttachment); an
// unfiltered Describe never surfaces tombstones, matching real AWS's
// list-vs-get behavior.
func (b *InMemoryBackend) DescribeTransitGatewayPeeringAttachments(
	ids []string,
) []*TransitGatewayPeeringAttachment {
	b.mu.RLock("DescribeTransitGatewayPeeringAttachments")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		return describeWithTombstones(b.tgwPeeringAttachments.All(), b.tgwPeeringAttachmentTombstones, ids,
			func(a *TransitGatewayPeeringAttachment) string { return a.TransitGatewayAttachmentID })
	}

	out := make([]*TransitGatewayPeeringAttachment, 0, b.tgwPeeringAttachments.Len())
	for _, att := range b.tgwPeeringAttachments.All() {
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
		Protocol:                            "gre",
		CreationTime:                        time.Now().UTC(),
	}
	b.tgwConnects.Put(conn)

	return conn, nil
}

// DeleteTransitGatewayConnect removes a TGW connect attachment.
func (b *InMemoryBackend) DeleteTransitGatewayConnect(id string) (*TransitGatewayConnect, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayConnect")
	defer b.mu.Unlock()

	conn, ok := b.tgwConnects.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayConnectNotFound, id)
	}
	cp := *conn
	b.tgwConnects.Delete(id)
	delete(b.tags, id)

	return &cp, nil
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

// tgwConnectPeerDefaultBgpAsn is applied when a Connect peer is created
// without BgpOptions.PeerAsn. AWS does not document an explicit default,
// but this mirrors tgwDefaultAmazonSideAsn, this codebase's existing
// default-ASN convention, so a Connect peer never surfaces a zero ASN.
const tgwConnectPeerDefaultBgpAsn = 64512

// Per api_op_CreateTransitGatewayConnectPeer.go, "The first address from
// the range must be configured on the appliance as the BGP IP address";
// the transit gateway side takes the next host address.
const (
	bgpAppliancePeerHostOffset  = 1
	bgpTransitGatewayHostOffset = 2
)

// CreateTransitGatewayConnectPeer creates a TGW connect peer. When
// transitGatewayAddress is empty, it is auto-assigned as the first host
// address of the parent transit gateway's first CIDR block, matching the
// documented default (api_op_CreateTransitGatewayConnectPeer.go:
// "If not specified, Amazon automatically assigns the first available IP
// address from the transit gateway CIDR block.").
func (b *InMemoryBackend) CreateTransitGatewayConnectPeer(
	connectAttachmentID, peerAddress, transitGatewayAddress string,
	insideCidrBlocks []string,
	bgpAsn int64,
) (*TransitGatewayConnectPeer, error) {
	if connectAttachmentID == "" || peerAddress == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayAttachmentId and PeerAddress are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	conn, ok := b.tgwConnects.Get(connectAttachmentID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayConnectNotFound, connectAttachmentID)
	}

	if transitGatewayAddress == "" {
		transitGatewayAddress = b.firstTGWCidrHostAddressLocked(conn.TransitGatewayID)
	}
	if transitGatewayAddress == "" && len(insideCidrBlocks) > 0 {
		// Real Connect peers auto-assign the GRE tunnel endpoint from InsideCidrBlocks when the
		// TGW has no TransitGatewayCidrBlocks; terraform's find/waiter treats empty as incomplete.
		transitGatewayAddress = firstCIDRHostAddress(insideCidrBlocks[0])
	}

	if bgpAsn == 0 {
		bgpAsn = tgwConnectPeerDefaultBgpAsn
	}

	tgwAsn := int64(tgwDefaultAmazonSideAsn)
	if tgw, foundTGW := b.transitGateways.Get(conn.TransitGatewayID); foundTGW {
		tgwAsn = tgw.Options.AmazonSideAsn
	}

	id := "tgw-connect-peer-" + uuid.New().String()[:8]
	peer := &TransitGatewayConnectPeer{
		TransitGatewayConnectPeerID: id,
		TransitGatewayAttachmentID:  connectAttachmentID,
		State:                       stateAvailable,
		InsideCidrBlocks:            insideCidrBlocks,
		PeerAddress:                 peerAddress,
		TransitGatewayAddress:       transitGatewayAddress,
		BgpConfigurations:           bgpConfigurationsForInsideCidrBlocks(insideCidrBlocks, bgpAsn, tgwAsn),
	}
	b.tgwConnectPeers.Put(peer)

	return peer, nil
}

// bgpConfigurationsForInsideCidrBlocks derives one BGP peering session per
// inside CIDR block: per api_op_CreateTransitGatewayConnectPeer.go, "The
// first address from the range must be configured on the appliance as the
// BGP IP address", so the appliance (PeerAddress) takes the first host and
// the transit gateway takes the second.
func bgpConfigurationsForInsideCidrBlocks(
	insideCidrBlocks []string,
	peerAsn, tgwAsn int64,
) []TransitGatewayBgpConfiguration {
	configs := make([]TransitGatewayBgpConfiguration, 0, len(insideCidrBlocks))
	for _, cidr := range insideCidrBlocks {
		configs = append(configs, TransitGatewayBgpConfiguration{
			BgpStatus:             "up",
			PeerAddress:           nthCIDRHostAddress(cidr, bgpAppliancePeerHostOffset),
			PeerAsn:               peerAsn,
			TransitGatewayAddress: nthCIDRHostAddress(cidr, bgpTransitGatewayHostOffset),
			TransitGatewayAsn:     tgwAsn,
		})
	}

	return configs
}

// firstTGWCidrHostAddressLocked returns the first host address of tgwID's
// first configured CIDR block, or "" if the transit gateway has none.
// Caller must hold b.mu.
func (b *InMemoryBackend) firstTGWCidrHostAddressLocked(tgwID string) string {
	tgw, ok := b.transitGateways.Get(tgwID)
	if !ok || len(tgw.Options.TransitGatewayCidrBlocks) == 0 {
		return ""
	}

	return firstCIDRHostAddress(tgw.Options.TransitGatewayCidrBlocks[0])
}

// firstCIDRHostAddress returns the first host address (network address + 1)
// of an IPv4 CIDR block, or "" if cidr doesn't parse as IPv4.
func firstCIDRHostAddress(cidr string) string {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return ""
	}

	ip := network.IP.To4()
	if ip == nil {
		return ""
	}

	host := make(net.IP, len(ip))
	copy(host, ip)
	host[len(host)-1]++

	return host.String()
}

// nthCIDRHostAddress returns the address at network+n within cidr, or "" if
// cidr doesn't parse. Unlike firstCIDRHostAddress it supports IPv6, needed
// for Connect peers' optional /125 InsideCidrBlocks entry. Safe for the /29
// (IPv4) and /125 (IPv6) blocks Connect peers require: n never overflows the
// last byte since those prefixes align to a byte boundary of 8 or fewer
// addresses.
func nthCIDRHostAddress(cidr string, n byte) string {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return ""
	}

	ip := network.IP
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}

	host := make(net.IP, len(ip))
	copy(host, ip)
	host[len(host)-1] += n

	return host.String()
}

// DeleteTransitGatewayConnectPeer removes a TGW connect peer.
func (b *InMemoryBackend) DeleteTransitGatewayConnectPeer(id string) (*TransitGatewayConnectPeer, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: TransitGatewayConnectPeerId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTransitGatewayConnectPeer")
	defer b.mu.Unlock()

	peer, ok := b.tgwConnectPeers.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayConnectPeerNotFound, id)
	}
	cp := *peer
	b.tgwConnectPeers.Delete(id)
	delete(b.tags, id)

	return &cp, nil
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
	routeTableID, prefixListID, attachmentID string,
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
		TransitGatewayAttachmentID: attachmentID,
		State:                      stateAvailable,
		Blackhole:                  blackhole,
	}
	b.tgwPrefixListRefs.Put(ref)

	return ref, nil
}

// DeleteTransitGatewayPrefixListReference removes a TGW prefix list reference.
func (b *InMemoryBackend) DeleteTransitGatewayPrefixListReference(
	routeTableID, prefixListID string,
) (*TransitGatewayPrefixListReference, error) {
	if routeTableID == "" || prefixListID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayRouteTableId and PrefixListId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DeleteTransitGatewayPrefixListReference")
	defer b.mu.Unlock()

	key := routeTableID + "/" + prefixListID

	ref, ok := b.tgwPrefixListRefs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrTGWPrefixListRefNotFound, routeTableID, prefixListID)
	}
	cp := *ref
	b.tgwPrefixListRefs.Delete(key)

	return &cp, nil
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
