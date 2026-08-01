package directconnect

import (
	"context"
)

// connectionOps returns the dispatch table for every Connection-family
// operation (create/describe/delete/update, hosting/allocation, LAG
// association, and MACsec key association -- 14 ops).
func (h *Handler) connectionOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateConnection":                  h.handleCreateConnection,
		"DeleteConnection":                  h.handleDeleteConnection,
		"UpdateConnection":                  h.handleUpdateConnection,
		"DescribeConnections":               h.handleDescribeConnections,
		"DescribeHostedConnections":         h.handleDescribeHostedConnections,
		"DescribeConnectionsOnInterconnect": h.handleDescribeConnectionsOnInterconnect,
		"ConfirmConnection":                 h.handleConfirmConnection,
		"AssociateConnectionWithLag":        h.handleAssociateConnectionWithLag,
		"DisassociateConnectionFromLag":     h.handleDisassociateConnectionFromLag,
		"AssociateHostedConnection":         h.handleAssociateHostedConnection,
		"AllocateConnectionOnInterconnect":  h.handleAllocateConnectionOnInterconnect,
		"AllocateHostedConnection":          h.handleAllocateHostedConnection,
		"AssociateMacSecKey":                h.handleAssociateMacSecKey,
		"DisassociateMacSecKey":             h.handleDisassociateMacSecKey,
		"DescribeConnectionLoa":             h.handleDescribeConnectionLoa,
		"DescribeLoa":                       h.handleDescribeLoa,
	}
}

func (h *Handler) handleCreateConnection(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createConnectionRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.CreateConnection(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleDeleteConnection(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[connectionIDRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.DeleteConnection(req.ConnectionID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleUpdateConnection(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateConnectionRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.UpdateConnection(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleDescribeConnections(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeConnectionsRequest](body)
	if err != nil {
		return nil, err
	}

	conns := h.Backend.DescribeConnections(req.ConnectionID)
	page, next := paginate(conns, req.NextToken, req.MaxResults)

	return marshalResponse(connectionsListResponse{Connections: connectionWires(page), NextToken: next})
}

func (h *Handler) handleDescribeHostedConnections(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeHostedConnectionsRequest](body)
	if err != nil {
		return nil, err
	}

	if req.ConnectionID == "" {
		return nil, clientError("connectionId is required")
	}

	conns := h.Backend.DescribeHostedConnections(req.ConnectionID)
	page, next := paginate(conns, req.NextToken, req.MaxResults)

	return marshalResponse(connectionsListResponse{Connections: connectionWires(page), NextToken: next})
}

func (h *Handler) handleDescribeConnectionsOnInterconnect(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[interconnectIDRequest](body)
	if err != nil {
		return nil, err
	}

	if req.InterconnectID == "" {
		return nil, clientError("interconnectId is required")
	}

	conns := h.Backend.DescribeConnectionsOnInterconnect(req.InterconnectID)

	return marshalResponse(connectionsListResponse{Connections: connectionWires(conns)})
}

func (h *Handler) handleConfirmConnection(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[connectionIDRequest](body)
	if err != nil {
		return nil, err
	}

	state, err := h.Backend.ConfirmConnection(req.ConnectionID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectionStateResponse{ConnectionState: state})
}

func (h *Handler) handleAssociateConnectionWithLag(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[associateConnectionWithLagRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.AssociateConnectionWithLag(req.ConnectionID, req.LagID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleDisassociateConnectionFromLag(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[disassociateConnectionFromLagRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.DisassociateConnectionFromLag(req.ConnectionID, req.LagID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleAssociateHostedConnection(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[associateHostedConnectionRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.AssociateHostedConnection(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleAllocateConnectionOnInterconnect(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[allocateConnectionOnInterconnectRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.AllocateConnectionOnInterconnect(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleAllocateHostedConnection(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[allocateHostedConnectionRequest](body)
	if err != nil {
		return nil, err
	}

	c, err := h.Backend.AllocateHostedConnection(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(toConnectionWire(c))
}

func (h *Handler) handleAssociateMacSecKey(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[associateMacSecKeyRequest](body)
	if err != nil {
		return nil, err
	}

	connID, keys, err := h.Backend.AssociateMacSecKey(req)
	if err != nil {
		return nil, err
	}

	return marshalResponse(associateMacSecKeyResponse{ConnectionID: connID, MacSecKeys: toMacSecKeyWire(keys)})
}

func (h *Handler) handleDisassociateMacSecKey(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[disassociateMacSecKeyRequest](body)
	if err != nil {
		return nil, err
	}

	if req.SecretARN == "" {
		return nil, clientError("secretARN is required")
	}

	connID, keys, err := h.Backend.DisassociateMacSecKey(req.ConnectionID, req.SecretARN)
	if err != nil {
		return nil, err
	}

	return marshalResponse(associateMacSecKeyResponse{ConnectionID: connID, MacSecKeys: toMacSecKeyWire(keys)})
}

func (h *Handler) handleDescribeConnectionLoa(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeLoaRequest](body)
	if err != nil {
		return nil, err
	}

	content, err := h.Backend.DescribeConnectionLoa(req.ConnectionID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(loaEnvelope{Loa: &loaWire{LoaContent: content, LoaContentType: LoaContentTypePdf}})
}

func (h *Handler) handleDescribeLoa(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[describeLoaRequest](body)
	if err != nil {
		return nil, err
	}

	content, err := h.Backend.DescribeLoa(req.ConnectionID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(describeLoaFlatResponse{LoaContent: content, LoaContentType: LoaContentTypePdf})
}

// connectionWires converts a slice of *Connection to its wire shape,
// nil-safe.
func connectionWires(cs []*Connection) []connectionWire {
	if len(cs) == 0 {
		return nil
	}

	out := make([]connectionWire, len(cs))
	for i, c := range cs {
		out[i] = toConnectionWire(c)
	}

	return out
}
