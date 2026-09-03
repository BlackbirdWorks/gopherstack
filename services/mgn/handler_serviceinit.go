package mgn

import (
	"context"
	"net/http"
)

func (h *Handler) handleInitializeService(_ context.Context, _ *http.Request, _ []byte) ([]byte, error) {
	h.Backend.InitializeService()

	return marshalResponse(struct{}{})
}

func (h *Handler) handleListManagedAccounts(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listManagedAccountsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListManagedAccounts(req.NextToken, int(req.MaxResults))
	if err != nil {
		return nil, err
	}

	items := make([]managedAccountWire, len(pg.Data))
	for i, a := range pg.Data {
		items[i] = managedAccountWire(a)
	}

	return marshalResponse(listManagedAccountsResponse{Items: items, NextToken: pg.Next})
}
