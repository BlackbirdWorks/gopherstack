package directconnect

import "context"

// ListVirtualInterfaceRoutes confirms the named virtual interface exists.
//
// gopherstack has no real BGP peering session, so a route table was never
// exchanged; fabricating one would violate the no-fabricated-data rule (see
// PARITY.md). The handler honestly returns an empty Routes list instead.
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
