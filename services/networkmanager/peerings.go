package networkmanager

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file implements PARITY.md family R: Peerings (4 ops).
// PeeringType has exactly one real value today (TRANSIT_GATEWAY), so the
// generic/subtype-specific op split (DeletePeering/ListPeerings vs.
// CreateTransitGatewayPeering/GetTransitGatewayPeering) is future-proofing
// rather than a live second case -- see PARITY.md.
//
// TransitGatewayArn is validated against services/ec2's real state via
// EC2Resolver (crossservice.go, same pattern as associations.go/
// attachments.go). TransitGatewayPeeringAttachmentId (real AWS's underlying
// EC2 transit gateway peering attachment ID) is left empty rather than
// fabricated, since this backend does not model that EC2-side resource.

func (b *InMemoryBackend) CreateTransitGatewayPeering(
	coreNetworkID, transitGatewayArn string, tagMap map[string]string,
) (*Peering, error) {
	b.mu.Lock("CreateTransitGatewayPeering")
	defer b.mu.Unlock()

	c, ok := b.coreNetworks.Get(coreNetworkID)
	if !ok {
		return nil, notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	if transitGatewayArn == "" {
		return nil, validationError("TransitGatewayArn is required")
	}

	if b.ec2Resolver != nil && !b.ec2Resolver.ResolveTransitGateway(transitGatewayArn) {
		return nil, notFoundError(resourceEC2TransitGateway, transitGatewayArn)
	}

	id := newPeeringID()
	p := &Peering{
		PeeringID:         id,
		ResourceArn:       b.peeringARN(id),
		CoreNetworkArn:    c.CoreNetworkArn,
		CoreNetworkID:     coreNetworkID,
		CreatedAt:         nowUTC(),
		OwnerAccountID:    b.accountID,
		PeeringType:       peeringTypeTransitGateway,
		State:             peeringStateCreating,
		TransitGatewayArn: transitGatewayArn,
		Tags:              tags.FromMap("networkmanager.peering."+id+".tags", tagMap),
	}
	b.peerings.Put(p)

	scheduleAdvance(b, "TransitGatewayPeeringAvailable", b.peerings, id,
		func(v *Peering) *string { return &v.State }, peeringStateCreating, peeringStateAvailable)

	return p.clone(), nil
}

func (b *InMemoryBackend) GetTransitGatewayPeering(id string) (*Peering, error) {
	b.mu.RLock("GetTransitGatewayPeering")
	defer b.mu.RUnlock()

	p, ok := b.peerings.Get(id)
	if !ok || p.PeeringType != peeringTypeTransitGateway {
		return nil, notFoundError(resourcePeering, id)
	}

	return p.clone(), nil
}

func (b *InMemoryBackend) DeletePeering(id string) (*Peering, error) {
	b.mu.Lock("DeletePeering")
	defer b.mu.Unlock()

	p, ok := b.peerings.Get(id)
	if !ok {
		return nil, notFoundError(resourcePeering, id)
	}

	p.State = peeringStateDeleting
	scheduleRemoval(b, "PeeringDeleted", b.peerings, id)

	return p.clone(), nil
}

func (b *InMemoryBackend) ListPeerings(
	coreNetworkID, edgeLocation, peeringType, state, token string, limit int,
) page.Page[*Peering] {
	b.mu.RLock("ListPeerings")
	defer b.mu.RUnlock()

	all := b.peerings.Snapshot()
	all = filterOptional(all, coreNetworkID, func(p *Peering) string { return p.CoreNetworkID })
	all = filterOptional(all, edgeLocation, func(p *Peering) string { return p.EdgeLocation })
	all = filterOptional(all, peeringType, func(p *Peering) string { return p.PeeringType })
	all = filterOptional(all, state, func(p *Peering) string { return p.State })

	return sortAndPage(all, func(p *Peering) string { return p.PeeringID }, (*Peering).clone, token, limit)
}
