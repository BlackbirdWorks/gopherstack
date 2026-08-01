package directconnect

import (
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateDirectConnectGateway creates a new, GLOBAL DirectConnectGateway
// (see store.go's GatewayARN). No tag-exceptions are modeled for this op
// despite it accepting Tags (confirmed by direct per-op grep -- PARITY.md),
// so unlike every other Tags-accepting Create* op in this service, this one
// does NOT call validateNewTags.
func (b *InMemoryBackend) CreateDirectConnectGateway(
	req *createDirectConnectGatewayRequest,
) (*DirectConnectGateway, error) {
	if req.DirectConnectGatewayName == "" {
		return nil, clientError("directConnectGatewayName is required")
	}

	b.mu.Lock("CreateDirectConnectGateway")
	defer b.mu.Unlock()

	id := newGatewayID()
	t := tags.New("directconnect.gateway." + id + ".tags")
	t.Merge(tagWireToMap(req.Tags))

	asn := b.nextAutoAmazonSideAsn()
	if req.AmazonSideAsn != nil {
		asn = *req.AmazonSideAsn
	}

	g := &DirectConnectGateway{
		DirectConnectGatewayID:    id,
		DirectConnectGatewayName:  req.DirectConnectGatewayName,
		DirectConnectGatewayState: GatewayStatePending,
		OwnerAccount:              b.accountID,
		AmazonSideAsn:             asn,
		Tags:                      t,
	}
	b.gateways.Put(g)

	b.scheduleTransition("gateway:"+id, []string{GatewayStateAvailable}, &g.DirectConnectGatewayState)

	return g.clone(), nil
}

// DescribeDirectConnectGateways returns gateways, optionally filtered by a
// single DirectConnectGatewayId.
func (b *InMemoryBackend) DescribeDirectConnectGateways(gatewayID string) []*DirectConnectGateway {
	b.mu.RLock("DescribeDirectConnectGateways")
	defer b.mu.RUnlock()

	if gatewayID != "" {
		if g, ok := b.gateways.Get(gatewayID); ok {
			return []*DirectConnectGateway{g.clone()}
		}

		return nil
	}

	all := b.gateways.Snapshot()
	out := make([]*DirectConnectGateway, 0, len(all))

	for _, g := range all {
		out = append(out, g.clone())
	}

	return out
}

// UpdateDirectConnectGateway renames a gateway -- the only Update* op in
// this service where the "what to change" field is itself required
// (PARITY.md).
func (b *InMemoryBackend) UpdateDirectConnectGateway(gatewayID, newName string) (*DirectConnectGateway, error) {
	if newName == "" {
		return nil, clientError("newDirectConnectGatewayName is required")
	}

	b.mu.Lock("UpdateDirectConnectGateway")
	defer b.mu.Unlock()

	g, ok := b.gateways.Get(gatewayID)
	if !ok {
		return nil, notFoundError(resourceGateway, gatewayID)
	}

	g.DirectConnectGatewayName = newName

	return g.clone(), nil
}

// DeleteDirectConnectGateway transitions a gateway to "deleting" then
// "deleted". Real AWS rejects deletion while associations/attachments
// exist; this backend enforces the same FK-integrity check.
func (b *InMemoryBackend) DeleteDirectConnectGateway(gatewayID string) (*DirectConnectGateway, error) {
	b.mu.Lock("DeleteDirectConnectGateway")
	defer b.mu.Unlock()

	g, ok := b.gateways.Get(gatewayID)
	if !ok {
		return nil, notFoundError(resourceGateway, gatewayID)
	}

	for _, a := range b.associations.All() {
		if a.DirectConnectGatewayID == gatewayID &&
			(a.AssociationState == AssociationStateAssociating || a.AssociationState == AssociationStateAssociated) {
			return nil, clientError("gateway " + gatewayID + " still has an active association: " + a.AssociationID)
		}
	}

	for _, v := range b.virtualInterfaces.Snapshot() {
		if v.DirectConnectGatewayID == gatewayID && v.VirtualInterfaceState != VifStateDeleted {
			return nil, clientError(
				"gateway " + gatewayID + " still has an attached virtual interface: " + v.VirtualInterfaceID,
			)
		}
	}

	g.DirectConnectGatewayState = GatewayStateDeleting
	b.scheduleTransition("gateway:"+gatewayID, []string{GatewayStateDeleted}, &g.DirectConnectGatewayState)

	return g.clone(), nil
}

// DescribeVirtualGateways returns every VGW usable for private VIF
// attachment. PARITY.md: this almost certainly maps 1:1 onto EC2's own
// VpnGateway records -- when an EC2GatewayResolver is wired in (see
// cli.go's wireDirectConnectEC2), this proxies from services/ec2's real
// VpnGateway list instead of maintaining a duplicate store; with no
// resolver wired (e.g. isolated unit tests), it returns an empty list
// rather than fabricating VGW ids with no backing resource.
func (b *InMemoryBackend) DescribeVirtualGateways() []VirtualGatewayInfo {
	b.mu.RLock("DescribeVirtualGateways")
	defer b.mu.RUnlock()

	if b.ec2Resolver == nil {
		return nil
	}

	ids := b.ec2Resolver.VirtualGateways()
	out := make([]VirtualGatewayInfo, 0, len(ids))

	for _, id := range ids {
		out = append(out, VirtualGatewayInfo{VirtualGatewayID: id, VirtualGatewayState: "available"})
	}

	return out
}

// VirtualGatewayInfo is the read-only shape DescribeVirtualGateways
// returns -- there is no backing internal model since this data is proxied
// from EC2, not stored by this backend (see DescribeVirtualGateways).
type VirtualGatewayInfo struct {
	VirtualGatewayID    string
	VirtualGatewayState string
}

// attachmentFromVIF derives a DirectConnectGatewayAttachment from a private
// or transit VIF's own state -- there is no dedicated Create*Attachment op
// in this SDK (PARITY.md), so attachments are computed at read time rather
// than independently stored.
func attachmentFromVIF(v *VirtualInterface) gatewayAttachmentWire {
	attachmentType := AttachmentTypePrivate
	if v.VirtualInterfaceType == VifTypeTransit {
		attachmentType = AttachmentTypeTransit
	}

	var state string

	switch v.VirtualInterfaceState {
	case VifStateConfirming, VifStateVerifying, VifStatePending:
		state = AttachmentStateAttaching
	case VifStateDeleting:
		state = AttachmentStateDetaching
	case VifStateDeleted, VifStateRejected:
		state = AttachmentStateDetached
	default:
		state = AttachmentStateAttached
	}

	return gatewayAttachmentWire{
		AttachmentState:              state,
		AttachmentType:               attachmentType,
		DirectConnectGatewayID:       v.DirectConnectGatewayID,
		VirtualInterfaceID:           v.VirtualInterfaceID,
		VirtualInterfaceOwnerAccount: v.OwnerAccount,
		VirtualInterfaceRegion:       v.Region,
	}
}

// DescribeDirectConnectGatewayAttachments returns the derived attachment
// list, filtered independently by gatewayID and/or vifID when non-empty.
func (b *InMemoryBackend) DescribeDirectConnectGatewayAttachments(gatewayID, vifID string) []gatewayAttachmentWire {
	b.mu.RLock("DescribeDirectConnectGatewayAttachments")
	defer b.mu.RUnlock()

	var out []gatewayAttachmentWire

	for _, v := range b.virtualInterfaces.Snapshot() {
		if v.VirtualInterfaceType == VifTypePublic || v.DirectConnectGatewayID == "" {
			continue
		}

		if gatewayID != "" && v.DirectConnectGatewayID != gatewayID {
			continue
		}

		if vifID != "" && v.VirtualInterfaceID != vifID {
			continue
		}

		out = append(out, attachmentFromVIF(v))
	}

	return out
}
