package directconnect

import "context"

// resiliencyGroupOps returns the dispatch table for the resiliency group operations.
func (h *Handler) resiliencyGroupOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateResiliencyGroup":                      h.handleCreateResiliencyGroup,
		"GetResiliencyGroup":                         h.handleGetResiliencyGroup,
		"ListResiliencyGroups":                       h.handleListResiliencyGroups,
		"UpdateResiliencyGroup":                      h.handleUpdateResiliencyGroup,
		"DeleteResiliencyGroup":                      h.handleDeleteResiliencyGroup,
		"AssociateConnectionsToResiliencyGroup":      h.handleAssociateConnectionsToResiliencyGroup,
		"DisassociateConnectionsFromResiliencyGroup": h.handleDisassociateConnectionsFromResiliencyGroup,
		"ListResiliencyGroupAssociations":            h.handleListResiliencyGroupAssociations,
		"UpdateConnectionsBillingMode":               h.handleUpdateConnectionsBillingMode,
	}
}

func (h *Handler) toResiliencyGroupWire(g *ResiliencyGroup) resiliencyGroupWire {
	return resiliencyGroupWire{
		OwnerAccount:        h.Backend.AccountID(),
		ResiliencyGroupArn:  h.Backend.ResiliencyGroupARN(g.ResiliencyGroupID),
		ResiliencyGroupID:   g.ResiliencyGroupID,
		ResiliencyGroupName: g.ResiliencyGroupName,
		ResiliencyGroupType: resiliencyGroupTypeManaged,
		State:               g.State,
		Tags:                toTagWire(g.Tags),
	}
}

func (h *Handler) toAssociationWire(a *ResiliencyGroupAssociation) resiliencyGroupAssociationWire {
	return resiliencyGroupAssociationWire{
		ConnectionArn:     h.Backend.ConnectionARN(a.ConnectionID),
		ResiliencyGroupID: a.ResiliencyGroupID,
		State:             a.State,
	}
}

func (h *Handler) handleCreateResiliencyGroup(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createResiliencyGroupRequest](body)
	if err != nil {
		return nil, err
	}

	g, err := h.Backend.CreateResiliencyGroup(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(resiliencyGroupResponse{ResiliencyGroup: h.toResiliencyGroupWire(g)})
}

func (h *Handler) handleGetResiliencyGroup(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resiliencyGroupIDRequest](body)
	if err != nil {
		return nil, err
	}

	g, err := h.Backend.GetResiliencyGroup(req.ResiliencyGroupID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(resiliencyGroupResponse{ResiliencyGroup: h.toResiliencyGroupWire(g)})
}

func (h *Handler) handleListResiliencyGroups(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[listResiliencyGroupsRequest](body)
	if err != nil {
		return nil, err
	}

	pg := h.Backend.ListResiliencyGroups(req.NextToken, req.MaxResults)
	items := make([]resiliencyGroupSummaryWire, 0, len(pg.Data))

	for _, g := range pg.Data {
		w := h.toResiliencyGroupWire(g)
		items = append(items, resiliencyGroupSummaryWire{
			OwnerAccount:        w.OwnerAccount,
			ResiliencyGroupArn:  w.ResiliencyGroupArn,
			ResiliencyGroupID:   w.ResiliencyGroupID,
			ResiliencyGroupName: w.ResiliencyGroupName,
			ResiliencyGroupType: w.ResiliencyGroupType,
			State:               w.State,
		})
	}

	return marshalResponse(resiliencyGroupsListResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleUpdateResiliencyGroup(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateResiliencyGroupRequest](body)
	if err != nil {
		return nil, err
	}

	g, err := h.Backend.UpdateResiliencyGroup(req.ResiliencyGroupID, req.ResiliencyGroupName)
	if err != nil {
		return nil, err
	}

	return marshalResponse(resiliencyGroupResponse{ResiliencyGroup: h.toResiliencyGroupWire(g)})
}

func (h *Handler) handleDeleteResiliencyGroup(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resiliencyGroupIDRequest](body)
	if err != nil {
		return nil, err
	}

	g, err := h.Backend.DeleteResiliencyGroup(req.ResiliencyGroupID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(resiliencyGroupResponse{ResiliencyGroup: h.toResiliencyGroupWire(g)})
}

func (h *Handler) associationsResponse(assocs []*ResiliencyGroupAssociation) ([]byte, error) {
	out := make([]resiliencyGroupAssociationWire, 0, len(assocs))
	for _, a := range assocs {
		out = append(out, h.toAssociationWire(a))
	}

	return marshalResponse(resiliencyGroupAssociationsResponse{ResiliencyGroupAssociations: out})
}

func (h *Handler) handleAssociateConnectionsToResiliencyGroup(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resiliencyGroupConnectionsRequest](body)
	if err != nil {
		return nil, err
	}

	assocs, err := h.Backend.AssociateConnectionsToResiliencyGroup(req.ResiliencyGroupID, req.ConnectionIdentifiers)
	if err != nil {
		return nil, err
	}

	return h.associationsResponse(assocs)
}

func (h *Handler) handleDisassociateConnectionsFromResiliencyGroup(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[resiliencyGroupConnectionsRequest](body)
	if err != nil {
		return nil, err
	}

	assocs, err := h.Backend.DisassociateConnectionsFromResiliencyGroup(
		req.ResiliencyGroupID, req.ConnectionIdentifiers)
	if err != nil {
		return nil, err
	}

	return h.associationsResponse(assocs)
}

func (h *Handler) handleListResiliencyGroupAssociations(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[listResiliencyGroupAssociationsRequest](body)
	if err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListResiliencyGroupAssociations(req.ResiliencyGroupID, req.NextToken, req.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]resiliencyGroupAssociationWire, 0, len(pg.Data))
	for _, a := range pg.Data {
		items = append(items, h.toAssociationWire(a))
	}

	return marshalResponse(resiliencyGroupAssociationsListResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleUpdateConnectionsBillingMode(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateConnectionsBillingModeRequest](body)
	if err != nil {
		return nil, err
	}

	conns, err := h.Backend.UpdateConnectionsBillingMode(req.BillingMode, req.ConnectionIDs)
	if err != nil {
		return nil, err
	}

	out := make([]connectionWire, 0, len(conns))
	for _, c := range conns {
		out = append(out, toConnectionWire(c))
	}

	return marshalResponse(updateConnectionsBillingModeResponse{BillingMode: req.BillingMode, Connections: out})
}
