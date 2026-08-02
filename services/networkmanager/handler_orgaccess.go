package networkmanager

import (
	"context"
	"net/http"
)

// orgAccessRoutes wires PARITY.md family V (2 ops).
func (h *Handler) orgAccessRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{"organizations", "service-access"},
			op:      "StartOrganizationServiceAccessUpdate",
			fn:      h.dispatchStartOrganizationServiceAccessUpdate,
		},
		{
			method:  http.MethodGet,
			pattern: []string{"organizations", "service-access"},
			op:      "ListOrganizationServiceAccessStatus",
			fn:      h.dispatchListOrganizationServiceAccessStatus,
		},
	}
}

func (h *Handler) dispatchStartOrganizationServiceAccessUpdate(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req startOrganizationServiceAccessUpdateReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	status := h.Backend.StartOrganizationServiceAccessUpdate(req.Action)

	return marshalResponse(organizationStatusEnvelope{OrganizationStatus: toOrganizationStatusWire(status)})
}

func (h *Handler) dispatchListOrganizationServiceAccessStatus(
	_ context.Context,
	r *http.Request,
	_ routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()
	status := h.Backend.ListOrganizationServiceAccessStatus()

	return marshalResponse(listOrganizationServiceAccessStatusResponse{
		OrganizationStatus: toOrganizationStatusWire(status), NextToken: queryNextToken(q),
	})
}
