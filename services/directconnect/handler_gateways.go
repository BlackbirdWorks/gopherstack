package directconnect

import "context"

// gatewayOps returns the dispatch table for every DirectConnectGateway,
// association, association-proposal, attachment, and virtual-gateway-proxy
// operation (14 ops).
func (h *Handler) gatewayOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateDirectConnectGateway":                       h.handleCreateGateway,
		"DescribeDirectConnectGateways":                    h.handleDescribeGateways,
		"DeleteDirectConnectGateway":                       h.handleDeleteGateway,
		"UpdateDirectConnectGateway":                       h.handleUpdateGateway,
		"CreateDirectConnectGatewayAssociation":            h.handleCreateAssociation,
		"DescribeDirectConnectGatewayAssociations":         h.handleDescribeAssociations,
		"DeleteDirectConnectGatewayAssociation":            h.handleDeleteAssociation,
		"UpdateDirectConnectGatewayAssociation":            h.handleUpdateAssociation,
		"CreateDirectConnectGatewayAssociationProposal":    h.handleCreateProposal,
		"DescribeDirectConnectGatewayAssociationProposals": h.handleDescribeProposals,
		"AcceptDirectConnectGatewayAssociationProposal":    h.handleAcceptProposal,
		"DeleteDirectConnectGatewayAssociationProposal":    h.handleDeleteProposal,
		"DescribeDirectConnectGatewayAttachments":          h.handleDescribeAttachments,
		"DescribeVirtualGateways":                          h.handleDescribeVirtualGateways,
	}
}

func (h *Handler) handleCreateGateway(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createDirectConnectGatewayRequest](body)
	if err != nil {
		return nil, err
	}

	g, err := h.Backend.CreateDirectConnectGateway(req)
	if err != nil {
		return nil, err
	}

	wire := toGatewayWire(g)

	return marshalResponse(gatewayEnvelope{DirectConnectGateway: &wire})
}

func (h *Handler) handleDescribeGateways(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeGatewaysRequest](body)
	if err != nil {
		return nil, err
	}

	gateways := h.Backend.DescribeDirectConnectGateways(req.DirectConnectGatewayID)
	pageGateways, next := paginate(gateways, req.NextToken, req.MaxResults)

	out := make([]directConnectGatewayWire, len(pageGateways))
	for i, g := range pageGateways {
		out[i] = toGatewayWire(g)
	}

	return marshalResponse(gatewaysListResponse{DirectConnectGateways: out, NextToken: next})
}

func (h *Handler) handleDeleteGateway(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[directConnectGatewayIDRequest](body)
	if err != nil {
		return nil, err
	}

	g, err := h.Backend.DeleteDirectConnectGateway(req.DirectConnectGatewayID)
	if err != nil {
		return nil, err
	}

	wire := toGatewayWire(g)

	return marshalResponse(gatewayEnvelope{DirectConnectGateway: &wire})
}

func (h *Handler) handleUpdateGateway(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateGatewayRequest](body)
	if err != nil {
		return nil, err
	}

	g, err := h.Backend.UpdateDirectConnectGateway(req.DirectConnectGatewayID, req.NewDirectConnectGatewayName)
	if err != nil {
		return nil, err
	}

	wire := toGatewayWire(g)

	return marshalResponse(gatewayEnvelope{DirectConnectGateway: &wire})
}

func (h *Handler) handleCreateAssociation(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createGatewayAssociationRequest](body)
	if err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateDirectConnectGatewayAssociation(req)
	if err != nil {
		return nil, err
	}

	wire := toAssociationWire(a)

	return marshalResponse(associationEnvelope{DirectConnectGatewayAssociation: &wire})
}

func (h *Handler) handleDescribeAssociations(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeGatewayAssociationsRequest](body)
	if err != nil {
		return nil, err
	}

	assocs := h.Backend.DescribeDirectConnectGatewayAssociations(
		req.AssociatedGatewayID, req.AssociationID, req.DirectConnectGatewayID, req.VirtualGatewayID,
	)
	pageAssocs, next := paginate(assocs, req.NextToken, req.MaxResults)

	out := make([]gatewayAssociationWire, len(pageAssocs))
	for i, a := range pageAssocs {
		out[i] = toAssociationWire(a)
	}

	return marshalResponse(gatewayAssociationsListResponse{DirectConnectGatewayAssociations: out, NextToken: next})
}

func (h *Handler) handleDeleteAssociation(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteGatewayAssociationRequest](body)
	if err != nil {
		return nil, err
	}

	a, err := h.Backend.DeleteDirectConnectGatewayAssociation(req)
	if err != nil {
		return nil, err
	}

	wire := toAssociationWire(a)

	return marshalResponse(associationEnvelope{DirectConnectGatewayAssociation: &wire})
}

func (h *Handler) handleUpdateAssociation(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateGatewayAssociationRequest](body)
	if err != nil {
		return nil, err
	}

	a, err := h.Backend.UpdateDirectConnectGatewayAssociation(req)
	if err != nil {
		return nil, err
	}

	wire := toAssociationWire(a)

	return marshalResponse(associationEnvelope{DirectConnectGatewayAssociation: &wire})
}

func (h *Handler) handleCreateProposal(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createGatewayAssociationProposalRequest](body)
	if err != nil {
		return nil, err
	}

	p, err := h.Backend.CreateDirectConnectGatewayAssociationProposal(req)
	if err != nil {
		return nil, err
	}

	wire := toProposalWire(p)

	return marshalResponse(proposalEnvelope{DirectConnectGatewayAssociationProposal: &wire})
}

func (h *Handler) handleDescribeProposals(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeGatewayAssociationProposalsRequest](body)
	if err != nil {
		return nil, err
	}

	proposals := h.Backend.DescribeDirectConnectGatewayAssociationProposals(
		req.AssociatedGatewayID, req.DirectConnectGatewayID, req.ProposalID,
	)
	pageProposals, next := paginate(proposals, req.NextToken, req.MaxResults)

	out := make([]gatewayAssociationProposalWire, len(pageProposals))
	for i, p := range pageProposals {
		out[i] = toProposalWire(p)
	}

	return marshalResponse(gatewayAssociationProposalsListResponse{
		DirectConnectGatewayAssociationProposals: out,
		NextToken:                                next,
	})
}

func (h *Handler) handleAcceptProposal(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[acceptGatewayAssociationProposalRequest](body)
	if err != nil {
		return nil, err
	}

	a, err := h.Backend.AcceptDirectConnectGatewayAssociationProposal(req)
	if err != nil {
		return nil, err
	}

	wire := toAssociationWire(a)

	return marshalResponse(associationEnvelope{DirectConnectGatewayAssociation: &wire})
}

func (h *Handler) handleDeleteProposal(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[proposalIDRequest](body)
	if err != nil {
		return nil, err
	}

	p, err := h.Backend.DeleteDirectConnectGatewayAssociationProposal(req.ProposalID)
	if err != nil {
		return nil, err
	}

	wire := toProposalWire(p)

	return marshalResponse(proposalEnvelope{DirectConnectGatewayAssociationProposal: &wire})
}

func (h *Handler) handleDescribeAttachments(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeGatewayAttachmentsRequest](body)
	if err != nil {
		return nil, err
	}

	attachments := h.Backend.DescribeDirectConnectGatewayAttachments(req.DirectConnectGatewayID, req.VirtualInterfaceID)
	pageAttachments, next := paginate(attachments, req.NextToken, req.MaxResults)

	return marshalResponse(gatewayAttachmentsListResponse{
		DirectConnectGatewayAttachments: pageAttachments,
		NextToken:                       next,
	})
}

func (h *Handler) handleDescribeVirtualGateways(_ context.Context, _ []byte) ([]byte, error) {
	vgws := h.Backend.DescribeVirtualGateways()
	out := make([]virtualGatewayWire, len(vgws))

	for i, v := range vgws {
		out[i] = virtualGatewayWire(v)
	}

	return marshalResponse(virtualGatewaysListResponse{VirtualGateways: out})
}
