package directconnect

import "context"

// lagAndInterconnectOps returns the dispatch table for every LAG and
// Interconnect operation (8 ops).
func (h *Handler) lagAndInterconnectOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateLag":               h.handleCreateLag,
		"DescribeLags":            h.handleDescribeLags,
		"DeleteLag":               h.handleDeleteLag,
		"UpdateLag":               h.handleUpdateLag,
		"CreateInterconnect":      h.handleCreateInterconnect,
		"DescribeInterconnects":   h.handleDescribeInterconnects,
		"DeleteInterconnect":      h.handleDeleteInterconnect,
		"DescribeInterconnectLoa": h.handleDescribeInterconnectLoa,
	}
}

func (h *Handler) handleCreateLag(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createLagRequest](body)
	if err != nil {
		return nil, err
	}

	l, err := h.Backend.CreateLag(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.toLagWireWithMembers(l))
}

func (h *Handler) handleDescribeLags(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeLagsRequest](body)
	if err != nil {
		return nil, err
	}

	lags := h.Backend.DescribeLags(req.LagID)
	pageLags, next := paginate(lags, req.NextToken, req.MaxResults)

	out := make([]lagWire, len(pageLags))
	for i, l := range pageLags {
		out[i] = h.toLagWireWithMembers(l)
	}

	return marshalResponse(lagsListResponse{Lags: out, NextToken: next})
}

func (h *Handler) handleDeleteLag(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[lagIDRequest](body)
	if err != nil {
		return nil, err
	}

	l, err := h.Backend.DeleteLag(req.LagID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.toLagWireWithMembers(l))
}

func (h *Handler) handleUpdateLag(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateLagRequest](body)
	if err != nil {
		return nil, err
	}

	l, err := h.Backend.UpdateLag(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(h.toLagWireWithMembers(l))
}

// toLagWireWithMembers builds a lag's wire shape including its live member
// Connections[] (see models.go's Lag doc comment for why they are looked up
// dynamically rather than stored on the Lag itself).
func (h *Handler) toLagWireWithMembers(l *Lag) lagWire {
	members := h.Backend.DescribeConnections("")

	var owned []*Connection

	for _, c := range members {
		if c.LagID == l.LagID {
			owned = append(owned, c)
		}
	}

	return toLagWire(l, owned)
}

func (h *Handler) handleCreateInterconnect(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createInterconnectRequest](body)
	if err != nil {
		return nil, err
	}

	ic, err := h.Backend.CreateInterconnect(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toInterconnectWire(ic))
}

func (h *Handler) handleDescribeInterconnects(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeInterconnectsRequest](body)
	if err != nil {
		return nil, err
	}

	ics := h.Backend.DescribeInterconnects(req.InterconnectID)
	pageICs, next := paginate(ics, req.NextToken, req.MaxResults)

	out := make([]interconnectWire, len(pageICs))
	for i, ic := range pageICs {
		out[i] = toInterconnectWire(ic)
	}

	return marshalResponse(interconnectsListResponse{Interconnects: out, NextToken: next})
}

func (h *Handler) handleDeleteInterconnect(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[interconnectIDRequest](body)
	if err != nil {
		return nil, err
	}

	state, err := h.Backend.DeleteInterconnect(req.InterconnectID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(interconnectStateResponse{InterconnectState: state})
}

func (h *Handler) handleDescribeInterconnectLoa(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeInterconnectLoaRequest](body)
	if err != nil {
		return nil, err
	}

	content, err := h.Backend.DescribeInterconnectLoa(req.InterconnectID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(loaEnvelope{Loa: &loaWire{LoaContent: content, LoaContentType: LoaContentTypePdf}})
}
