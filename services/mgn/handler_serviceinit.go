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

	accounts, err := h.Backend.ListManagedAccounts()
	if err != nil {
		return nil, err
	}

	items := make([]managedAccountWire, len(accounts))
	for i, a := range accounts {
		items[i] = managedAccountWire(a)
	}

	return marshalResponse(listManagedAccountsResponse{Items: items})
}
