package directconnect

import "strings"

// resolveGatewayTypeLocked determines whether id names a transit gateway or
// a virtual private gateway. When an EC2GatewayResolver is wired in (see
// store.go), it validates id against the real services/ec2 backend rather
// than guessing. With no resolver wired, it falls back to services/ec2's
// own real, documented ID-prefix convention ("tgw-"/"vgw-", see
// services/ec2/resource_ids.go) -- a real, checkable convention, not an
// arbitrary guess, but NOT independently validated against an actual EC2
// resource in that fallback path.
func (b *InMemoryBackend) resolveGatewayTypeLocked(id string) (string, bool) {
	if b.ec2Resolver != nil {
		if b.ec2Resolver.ResolveTransitGateway(id) {
			return GatewayTypeTransitGateway, true
		}

		if b.ec2Resolver.ResolveVpnGateway(id) {
			return GatewayTypeVirtualPrivateGateway, true
		}

		return "", false
	}

	switch {
	case strings.HasPrefix(id, "tgw-"):
		return GatewayTypeTransitGateway, true
	case strings.HasPrefix(id, "vgw-"):
		return GatewayTypeVirtualPrivateGateway, true
	default:
		return "", false
	}
}

// CreateDirectConnectGatewayAssociation associates a DirectConnectGateway
// (same-account) with a virtual private gateway or transit gateway. Accepts
// either GatewayId (generic, either kind) or the older VirtualGatewayId
// (VGW-only) -- neither required at the struct level (PARITY.md wire-trap
// #3); this checks whichever field is actually present rather than
// assuming one canonical addressing mode.
func (b *InMemoryBackend) CreateDirectConnectGatewayAssociation(
	req *createGatewayAssociationRequest,
) (*GatewayAssociation, error) {
	if req.DirectConnectGatewayID == "" {
		return nil, clientError("directConnectGatewayId is required")
	}

	targetID := req.GatewayID
	if targetID == "" {
		targetID = req.VirtualGatewayID
	}

	if targetID == "" {
		return nil, clientError("gatewayId or virtualGatewayId is required")
	}

	b.mu.Lock("CreateDirectConnectGatewayAssociation")
	defer b.mu.Unlock()

	if _, ok := b.gateways.Get(req.DirectConnectGatewayID); !ok {
		return nil, notFoundError(resourceGateway, req.DirectConnectGatewayID)
	}

	gatewayType, ok := b.resolveGatewayTypeLocked(targetID)
	if !ok {
		return nil, notFoundError("virtual private gateway or transit gateway", targetID)
	}

	id := newAssociationID()
	a := &GatewayAssociation{
		AssociationID:                    id,
		DirectConnectGatewayID:           req.DirectConnectGatewayID,
		DirectConnectGatewayOwnerAccount: b.accountID,
		AssociationState:                 AssociationStateAssociating,
		GatewayID:                        targetID,
		GatewayOwnerAccount:              b.accountID,
		GatewayRegion:                    b.region,
		GatewayType:                      gatewayType,
		AllowedPrefixes:                  fromRouteFilterPrefixWire(req.AddAllowedPrefixesToDirectConnectGateway),
	}
	b.associations.Put(a)

	b.scheduleTransition("association:"+id, []string{AssociationStateAssociated}, &a.AssociationState)

	return a.clone(), nil
}

// resolveAssociationLocked resolves either AssociationId alone or the
// (DirectConnectGatewayId, VirtualGatewayId) pair to a stored
// GatewayAssociation -- PARITY.md wire-trap #3's DeleteDirectConnectGatewayAssociation
// variant of the same addressing ambiguity.
func (b *InMemoryBackend) resolveAssociationLocked(
	associationID, gatewayID, virtualGatewayID string,
) (*GatewayAssociation, bool) {
	if associationID != "" {
		return b.associations.Get(associationID)
	}

	for _, a := range b.associations.All() {
		if a.DirectConnectGatewayID == gatewayID && a.GatewayID == virtualGatewayID {
			return a, true
		}
	}

	return nil, false
}

// DeleteDirectConnectGatewayAssociation disassociates gateway from its VGW/
// TGW, addressed either by AssociationId alone or the
// (DirectConnectGatewayId, VirtualGatewayId) pair.
func (b *InMemoryBackend) DeleteDirectConnectGatewayAssociation(
	req *deleteGatewayAssociationRequest,
) (*GatewayAssociation, error) {
	b.mu.Lock("DeleteDirectConnectGatewayAssociation")
	defer b.mu.Unlock()

	a, ok := b.resolveAssociationLocked(req.AssociationID, req.DirectConnectGatewayID, req.VirtualGatewayID)
	if !ok {
		return nil, notFoundError(resourceAssociation, req.AssociationID)
	}

	a.AssociationState = AssociationStateDisassociating
	b.scheduleTransition("association:"+a.AssociationID, []string{AssociationStateDisassociated}, &a.AssociationState)

	return a.clone(), nil
}

// UpdateDirectConnectGatewayAssociation adds/removes allowed prefixes on an
// existing association, transitioning through "updating" -- the only state
// enum in this service with a mid-lifecycle value (PARITY.md).
func (b *InMemoryBackend) UpdateDirectConnectGatewayAssociation(
	req *updateGatewayAssociationRequest,
) (*GatewayAssociation, error) {
	if req.AssociationID == "" {
		return nil, clientError("associationId is required")
	}

	b.mu.Lock("UpdateDirectConnectGatewayAssociation")
	defer b.mu.Unlock()

	a, ok := b.associations.Get(req.AssociationID)
	if !ok {
		return nil, notFoundError(resourceAssociation, req.AssociationID)
	}

	a.AllowedPrefixes = applyPrefixDelta(
		a.AllowedPrefixes,
		fromRouteFilterPrefixWire(req.AddAllowedPrefixesToDirectConnectGateway),
		fromRouteFilterPrefixWire(req.RemoveAllowedPrefixesToDirectConnectGateway),
	)

	prevState := a.AssociationState
	a.AssociationState = AssociationStateUpdating
	b.scheduleTransition("association:"+req.AssociationID, []string{prevState}, &a.AssociationState)

	return a.clone(), nil
}

// applyPrefixDelta returns current with add appended (skipping duplicates
// already present) and every prefix in remove filtered out.
func applyPrefixDelta(current, add, remove []RouteFilterPrefix) []RouteFilterPrefix {
	out := make([]RouteFilterPrefix, 0, len(current)+len(add))

	removeSet := make(map[string]bool, len(remove))
	for _, r := range remove {
		removeSet[r.Cidr] = true
	}

	existing := make(map[string]bool, len(current))

	for _, p := range current {
		if removeSet[p.Cidr] {
			continue
		}

		out = append(out, p)
		existing[p.Cidr] = true
	}

	for _, p := range add {
		if existing[p.Cidr] {
			continue
		}

		out = append(out, p)
		existing[p.Cidr] = true
	}

	return out
}

// DescribeDirectConnectGatewayAssociations returns associations, filtered
// independently by AssociatedGatewayId/AssociationId/DirectConnectGatewayId/
// VirtualGatewayId when non-empty (PARITY.md: four independent optional
// filters, any combination).
func (b *InMemoryBackend) DescribeDirectConnectGatewayAssociations(
	associatedGatewayID, associationID, gatewayID, virtualGatewayID string,
) []*GatewayAssociation {
	b.mu.RLock("DescribeDirectConnectGatewayAssociations")
	defer b.mu.RUnlock()

	out := make([]*GatewayAssociation, 0, b.associations.Len())

	for _, a := range b.associations.Snapshot() {
		if associationID != "" && a.AssociationID != associationID {
			continue
		}

		if gatewayID != "" && a.DirectConnectGatewayID != gatewayID {
			continue
		}

		if associatedGatewayID != "" && a.GatewayID != associatedGatewayID {
			continue
		}

		if virtualGatewayID != "" && a.GatewayID != virtualGatewayID {
			continue
		}

		out = append(out, a.clone())
	}

	return out
}

// CreateDirectConnectGatewayAssociationProposal creates a cross-account
// proposal from the (implicit, single-account-simulated) VGW/TGW owner to
// the named DirectConnectGatewayOwnerAccount -- see PARITY.md's
// cross-account proposal flow notes.
func (b *InMemoryBackend) CreateDirectConnectGatewayAssociationProposal(
	req *createGatewayAssociationProposalRequest,
) (*GatewayAssociationProposal, error) {
	if req.DirectConnectGatewayID == "" || req.DirectConnectGatewayOwnerAccount == "" || req.GatewayID == "" {
		return nil, clientError("directConnectGatewayId, directConnectGatewayOwnerAccount, and gatewayId are required")
	}

	b.mu.Lock("CreateDirectConnectGatewayAssociationProposal")
	defer b.mu.Unlock()

	gatewayType, ok := b.resolveGatewayTypeLocked(req.GatewayID)
	if !ok {
		return nil, notFoundError("virtual private gateway or transit gateway", req.GatewayID)
	}

	id := newProposalID()
	p := &GatewayAssociationProposal{
		ProposalID:                       id,
		DirectConnectGatewayID:           req.DirectConnectGatewayID,
		DirectConnectGatewayOwnerAccount: req.DirectConnectGatewayOwnerAccount,
		ProposalState:                    ProposalStateRequested,
		GatewayID:                        req.GatewayID,
		GatewayOwnerAccount:              b.accountID,
		GatewayRegion:                    b.region,
		GatewayType:                      gatewayType,
		RequestedAllowedPrefixes:         fromRouteFilterPrefixWire(req.AddAllowedPrefixesToDirectConnectGateway),
	}
	b.proposals.Put(p)

	return p.clone(), nil
}

// AcceptDirectConnectGatewayAssociationProposal accepts a pending proposal,
// creating the resulting association. ASSUMPTION, documented per PARITY.md:
// AssociatedGatewayOwnerAccount is treated as the accepter's confirmation of
// who owns the associated (VGW/TGW) gateway -- i.e. it should match the
// proposal's own GatewayOwnerAccount -- rather than the accepter's own
// account identity. This backend simulates only one AWS account at a time,
// so it cannot cross-validate this against a second, real caller identity;
// it is recorded and required to be non-empty, not strictly checked against
// a second account. This is a defensible reading of the field names, NOT a
// verified AWS behavior (see PARITY.md's top-5-hardest-things section).
func (b *InMemoryBackend) AcceptDirectConnectGatewayAssociationProposal(
	req *acceptGatewayAssociationProposalRequest,
) (*GatewayAssociation, error) {
	if req.AssociatedGatewayOwnerAccount == "" || req.DirectConnectGatewayID == "" || req.ProposalID == "" {
		return nil, clientError("associatedGatewayOwnerAccount, directConnectGatewayId, and proposalId are required")
	}

	b.mu.Lock("AcceptDirectConnectGatewayAssociationProposal")
	defer b.mu.Unlock()

	p, ok := b.proposals.Get(req.ProposalID)
	if !ok {
		return nil, notFoundError(resourceProposal, req.ProposalID)
	}

	if p.DirectConnectGatewayID != req.DirectConnectGatewayID {
		return nil, clientError("proposal " + req.ProposalID + " does not target gateway " + req.DirectConnectGatewayID)
	}

	if p.ProposalState != ProposalStateRequested {
		return nil, clientError("proposal " + req.ProposalID + " is not awaiting acceptance")
	}

	allowed := p.RequestedAllowedPrefixes
	if len(req.OverrideAllowedPrefixesToDirectConnectGateway) > 0 {
		allowed = fromRouteFilterPrefixWire(req.OverrideAllowedPrefixesToDirectConnectGateway)
	}

	p.ProposalState = ProposalStateAccepted

	id := newAssociationID()
	a := &GatewayAssociation{
		AssociationID:                    id,
		DirectConnectGatewayID:           p.DirectConnectGatewayID,
		DirectConnectGatewayOwnerAccount: p.DirectConnectGatewayOwnerAccount,
		AssociationState:                 AssociationStateAssociating,
		GatewayID:                        p.GatewayID,
		GatewayOwnerAccount:              req.AssociatedGatewayOwnerAccount,
		GatewayRegion:                    p.GatewayRegion,
		GatewayType:                      p.GatewayType,
		AllowedPrefixes:                  allowed,
	}
	b.associations.Put(a)

	b.scheduleTransition("association:"+id, []string{AssociationStateAssociated}, &a.AssociationState)

	return a.clone(), nil
}

// DescribeDirectConnectGatewayAssociationProposals returns proposals,
// filtered independently by AssociatedGatewayId/DirectConnectGatewayId/
// ProposalId when non-empty.
func (b *InMemoryBackend) DescribeDirectConnectGatewayAssociationProposals(
	associatedGatewayID, gatewayID, proposalID string,
) []*GatewayAssociationProposal {
	b.mu.RLock("DescribeDirectConnectGatewayAssociationProposals")
	defer b.mu.RUnlock()

	out := make([]*GatewayAssociationProposal, 0, b.proposals.Len())

	for _, p := range b.proposals.Snapshot() {
		if proposalID != "" && p.ProposalID != proposalID {
			continue
		}

		if gatewayID != "" && p.DirectConnectGatewayID != gatewayID {
			continue
		}

		if associatedGatewayID != "" && p.GatewayID != associatedGatewayID {
			continue
		}

		out = append(out, p.clone())
	}

	return out
}

// DeleteDirectConnectGatewayAssociationProposal withdraws a pending
// proposal.
func (b *InMemoryBackend) DeleteDirectConnectGatewayAssociationProposal(
	proposalID string,
) (*GatewayAssociationProposal, error) {
	b.mu.Lock("DeleteDirectConnectGatewayAssociationProposal")
	defer b.mu.Unlock()

	p, ok := b.proposals.Get(proposalID)
	if !ok {
		return nil, notFoundError(resourceProposal, proposalID)
	}

	p.ProposalState = ProposalStateDeleted

	return p.clone(), nil
}
