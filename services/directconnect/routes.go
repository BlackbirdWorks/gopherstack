package directconnect

import "context"

// ListVirtualInterfaceRoutes confirms the named virtual interface exists.
//
// gopherstack has no real BGP peering session with a customer network -- the
// BGPPeer records this backend stores (bgp.go) track configuration (ASN,
// auth key, address family) but never a live route table exchanged over an
// actual link. Fabricating a plausible-looking accepted/advertised route
// list for a session that was never really established would violate this
// project's no-fabricated-data rule (see PARITY.md's honest-gap section).
// Instead this method validates the request and confirms the virtual
// interface genuinely exists (both real backend state); the handler then
// honestly returns an empty Routes list -- exactly what a virtual interface
// with no live route exchange legitimately has.
func (b *InMemoryBackend) ListVirtualInterfaceRoutes(vifID string) error {
	if vifID == "" {
		return clientError("virtualInterfaceId is required")
	}

	b.mu.RLock("ListVirtualInterfaceRoutes")
	defer b.mu.RUnlock()

	if !b.virtualInterfaces.Has(vifID) {
		return notFoundError(resourceVif, vifID)
	}

	return nil
}

func (h *Handler) handleListVirtualInterfaceRoutes(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[listVirtualInterfaceRoutesRequest](body)
	if err != nil {
		return nil, err
	}

	if vErr := h.Backend.ListVirtualInterfaceRoutes(req.VirtualInterfaceID); vErr != nil {
		return nil, vErr
	}

	return marshalResponse(listVirtualInterfaceRoutesResponse{
		VirtualInterfaceID: req.VirtualInterfaceID,
		Routes:             []routeWire{},
	})
}
