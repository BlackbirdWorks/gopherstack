package directconnect

import "context"

// vifOps returns the dispatch table for every VirtualInterface operation
// (create/allocate/confirm/associate/update/delete/describe across
// private/public/transit -- 13 ops).
func (h *Handler) vifOps() map[string]opFunc {
	return map[string]opFunc{
		"CreatePrivateVirtualInterface":    h.handleCreatePrivateVif,
		"CreatePublicVirtualInterface":     h.handleCreatePublicVif,
		"CreateTransitVirtualInterface":    h.handleCreateTransitVif,
		"AllocatePrivateVirtualInterface":  h.handleAllocatePrivateVif,
		"AllocatePublicVirtualInterface":   h.handleAllocatePublicVif,
		"AllocateTransitVirtualInterface":  h.handleAllocateTransitVif,
		"ConfirmPrivateVirtualInterface":   h.handleConfirmPrivateVif,
		"ConfirmPublicVirtualInterface":    h.handleConfirmPublicVif,
		"ConfirmTransitVirtualInterface":   h.handleConfirmTransitVif,
		"AssociateVirtualInterface":        h.handleAssociateVif,
		"UpdateVirtualInterfaceAttributes": h.handleUpdateVifAttributes,
		"DeleteVirtualInterface":           h.handleDeleteVif,
		"DescribeVirtualInterfaces":        h.handleDescribeVifs,
	}
}

// vifWireWithAsn builds v's flattened/nested wire shape with AmazonSideAsn
// resolved from its bound DirectConnectGateway (see models.go's
// VirtualInterface doc comment).
func (h *Handler) vifWireWithAsn(v *VirtualInterface) virtualInterfaceWire {
	asn := h.Backend.GatewayAmazonSideAsn(v.DirectConnectGatewayID)

	return toVirtualInterfaceWire(v, asn)
}

func (h *Handler) handleCreatePrivateVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createPrivateVifRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.CreatePrivateVirtualInterface(req.ConnectionID, req.NewPrivateVirtualInterface)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.vifWireWithAsn(v))
}

func (h *Handler) handleCreatePublicVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createPublicVifRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.CreatePublicVirtualInterface(req.ConnectionID, req.NewPublicVirtualInterface)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.vifWireWithAsn(v))
}

func (h *Handler) handleCreateTransitVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createTransitVifRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.CreateTransitVirtualInterface(req.ConnectionID, req.NewTransitVirtualInterface)
	if err != nil {
		return nil, err
	}

	wire := h.vifWireWithAsn(v)

	return marshalResponse(vifEnvelope{VirtualInterface: &wire})
}

func (h *Handler) handleAllocatePrivateVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[allocatePrivateVifRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.AllocatePrivateVirtualInterface(
		req.ConnectionID, req.OwnerAccount, req.NewPrivateVirtualInterfaceAllocation,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.vifWireWithAsn(v))
}

func (h *Handler) handleAllocatePublicVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[allocatePublicVifRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.AllocatePublicVirtualInterface(
		req.ConnectionID, req.OwnerAccount, req.NewPublicVirtualInterfaceAllocation,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.vifWireWithAsn(v))
}

func (h *Handler) handleAllocateTransitVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[allocateTransitVifRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.AllocateTransitVirtualInterface(
		req.ConnectionID, req.OwnerAccount, req.NewTransitVirtualInterfaceAllocation,
	)
	if err != nil {
		return nil, err
	}

	wire := h.vifWireWithAsn(v)

	return marshalResponse(vifEnvelope{VirtualInterface: &wire})
}

func (h *Handler) handleConfirmPrivateVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[confirmPrivateVifRequest](body)
	if err != nil {
		return nil, err
	}

	state, err := h.Backend.ConfirmPrivateVirtualInterface(
		req.VirtualInterfaceID, req.DirectConnectGatewayID, req.VirtualGatewayID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(vifStateResponse{VirtualInterfaceState: state})
}

func (h *Handler) handleConfirmPublicVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[confirmPublicVifRequest](body)
	if err != nil {
		return nil, err
	}

	state, err := h.Backend.ConfirmPublicVirtualInterface(req.VirtualInterfaceID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(vifStateResponse{VirtualInterfaceState: state})
}

func (h *Handler) handleConfirmTransitVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[confirmTransitVifRequest](body)
	if err != nil {
		return nil, err
	}

	state, err := h.Backend.ConfirmTransitVirtualInterface(req.VirtualInterfaceID, req.DirectConnectGatewayID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(vifStateResponse{VirtualInterfaceState: state})
}

func (h *Handler) handleAssociateVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[associateVifRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.AssociateVirtualInterface(req.VirtualInterfaceID, req.ConnectionID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.vifWireWithAsn(v))
}

func (h *Handler) handleUpdateVifAttributes(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateVifAttributesRequest](body)
	if err != nil {
		return nil, err
	}

	v, err := h.Backend.UpdateVirtualInterfaceAttributes(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.vifWireWithAsn(v))
}

func (h *Handler) handleDeleteVif(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[virtualInterfaceIDRequest](body)
	if err != nil {
		return nil, err
	}

	state, err := h.Backend.DeleteVirtualInterface(req.VirtualInterfaceID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(vifStateResponse{VirtualInterfaceState: state})
}

func (h *Handler) handleDescribeVifs(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeVifsRequest](body)
	if err != nil {
		return nil, err
	}

	vifs := h.Backend.DescribeVirtualInterfaces(req.ConnectionID, req.VirtualInterfaceID)
	pageVifs, next := paginate(vifs, req.NextToken, req.MaxResults)

	out := make([]virtualInterfaceWire, len(pageVifs))
	for i, v := range pageVifs {
		out[i] = h.vifWireWithAsn(v)
	}

	return marshalResponse(vifsListResponse{VirtualInterfaces: out, NextToken: next})
}
