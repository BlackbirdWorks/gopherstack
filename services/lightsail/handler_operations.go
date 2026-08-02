package lightsail

import "context"

// operationOps returns the dispatch table for family Z (3 ops).
func (h *Handler) operationOps() map[string]opFunc {
	return map[string]opFunc{
		"GetOperation":             h.handleGetOperation,
		"GetOperations":            h.handleGetOperations,
		"GetOperationsForResource": h.handleGetOperationsForResource,
	}
}

type getOperationRequest struct {
	OperationID string `json:"operationId"`
}

func (h *Handler) handleGetOperation(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getOperationRequest](body)
	if err != nil {
		return nil, err
	}

	op, getErr := h.Backend.GetOperation(req.OperationID)
	if getErr != nil {
		return nil, getErr
	}

	return marshalResponse(opEnvelope(op))
}

type operationsListResponse struct {
	NextPageToken string          `json:"nextPageToken,omitempty"`
	Operations    []operationWire `json:"operations,omitempty"`
}

func (h *Handler) handleGetOperations(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetOperations(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	return marshalResponse(
		operationsListResponse{NextPageToken: pg.Next, Operations: operationsToWire(derefOps(pg.Data))},
	)
}

type getOperationsForResourceRequest struct {
	ResourceName string `json:"resourceName"`
	PageToken    string `json:"pageToken,omitempty"`
}

type operationsForResourceResponse struct {
	NextPageToken string          `json:"nextPageToken,omitempty"`
	NextPageCount string          `json:"nextPageCount,omitempty"`
	Operations    []operationWire `json:"operations,omitempty"`
}

func (h *Handler) handleGetOperationsForResource(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getOperationsForResourceRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetOperationsForResource(req.ResourceName, req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	return marshalResponse(
		operationsForResourceResponse{NextPageToken: pg.Next, Operations: operationsToWire(derefOps(pg.Data))},
	)
}

func derefOps(in []*Operation) []Operation {
	out := make([]Operation, len(in))
	for i, o := range in {
		out[i] = *o
	}

	return out
}
