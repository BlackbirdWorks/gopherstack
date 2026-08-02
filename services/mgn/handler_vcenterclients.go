package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleDescribeVcenterClients(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	pg, err := h.Backend.DescribeVcenterClients(queryNextToken(q), int(queryMaxResults(q)))
	if err != nil {
		return nil, err
	}

	items := make([]vcenterClientWire, len(pg.Data))
	for i, v := range pg.Data {
		items[i] = toVcenterClientWire(v)
	}

	return marshalResponse(describeVcenterClientsResponse{Items: items, NextToken: pg.Next})
}

func (h *Handler) handleDeleteVcenterClient(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req vcenterClientIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteVcenterClient(req.VcenterClientID); err != nil {
		return nil, err
	}

	return marshalResponse(struct{}{})
}
