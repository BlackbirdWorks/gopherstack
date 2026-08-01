package directconnect

// CreateBGPPeer adds a new BGP peer to the named VirtualInterface's
// BgpPeers list. VirtualInterfaceId is not marked required at the Go
// struct level (PARITY.md), but this backend cannot honor the op without
// it.
func (b *InMemoryBackend) CreateBGPPeer(vifID string, n *newBGPPeerWire) (*VirtualInterface, error) {
	if vifID == "" {
		return nil, clientError("virtualInterfaceId is required")
	}

	if n == nil {
		return nil, clientError("newBGPPeer is required")
	}

	b.mu.Lock("CreateBGPPeer")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(vifID)
	if !ok {
		return nil, notFoundError(resourceVif, vifID)
	}

	asnValue, asnProvided := resolveAsnInput(n.Asn, n.AsnLong)

	initialState := BGPPeerStatePending
	if v.VirtualInterfaceType == VifTypePublic {
		initialState = BGPPeerStateVerifying
	}

	peer := &BGPPeer{
		BgpPeerID:       newBgpPeerID(),
		BgpPeerState:    initialState,
		BgpStatus:       BGPStatusUnknown,
		AddressFamily:   n.AddressFamily,
		AmazonAddress:   n.AmazonAddress,
		CustomerAddress: n.CustomerAddress,
		AuthKey:         n.AuthKey,
		AsnValue:        asnValue,
		AsnProvided:     asnProvided,
	}
	v.BgpPeers = append(v.BgpPeers, peer)

	component := "bgppeer:" + peer.BgpPeerID
	b.scheduleTransition(component, []string{BGPPeerStateAvailable}, &peer.BgpPeerState)
	b.scheduleTransition(component+":status", []string{BGPStatusUp}, &peer.BgpStatus)

	return v.clone(), nil
}

// bgpPeerMatchesLocked reports whether peer is identified by the
// (partial, caller-supplied) combination DeleteBGPPeer accepted -- see
// PARITY.md: none of Asn/AsnLong/BgpPeerId/CustomerAddress is individually
// required, so this backend matches on whichever subset the caller
// supplied, requiring at least one to avoid deleting an arbitrary peer.
func bgpPeerMatchesLocked(peer *BGPPeer, bgpPeerID, customerAddress string, asnValue int64, asnProvided bool) bool {
	if bgpPeerID != "" {
		return peer.BgpPeerID == bgpPeerID
	}

	matched := false

	if asnProvided {
		if peer.AsnValue != asnValue {
			return false
		}

		matched = true
	}

	if customerAddress != "" {
		if peer.CustomerAddress != customerAddress {
			return false
		}

		matched = true
	}

	return matched
}

// DeleteBGPPeer removes the BGP peer identified by the given combination of
// Asn/AsnLong/BgpPeerId/CustomerAddress from vifID's BgpPeers list.
func (b *InMemoryBackend) DeleteBGPPeer(req *deleteBGPPeerRequest) (*VirtualInterface, error) {
	if req.VirtualInterfaceID == "" {
		return nil, clientError("virtualInterfaceId is required")
	}

	asnValue, asnProvided := resolveAsnInput(req.Asn, req.AsnLong)
	if req.BgpPeerID == "" && !asnProvided && req.CustomerAddress == "" {
		return nil, clientError("at least one of asn, asnLong, bgpPeerId, or customerAddress is required")
	}

	b.mu.Lock("DeleteBGPPeer")
	defer b.mu.Unlock()

	v, ok := b.virtualInterfaces.Get(req.VirtualInterfaceID)
	if !ok {
		return nil, notFoundError(resourceVif, req.VirtualInterfaceID)
	}

	found := false

	for _, peer := range v.BgpPeers {
		if !bgpPeerMatchesLocked(peer, req.BgpPeerID, req.CustomerAddress, asnValue, asnProvided) {
			continue
		}

		found = true
		peer.BgpPeerState = BGPPeerStateDeleting
		b.scheduleTransition("bgppeer:"+peer.BgpPeerID, []string{BGPPeerStateDeleted}, &peer.BgpPeerState)
	}

	if !found {
		return nil, notFoundError(resourceBGPPeer, req.BgpPeerID)
	}

	return v.clone(), nil
}
