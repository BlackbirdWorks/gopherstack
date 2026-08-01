package outposts

import (
	"context"
	"net/http"
)

func (h *Handler) handleCreateSite(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req createSiteRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.CreateSite(&req)
	if err != nil {
		return nil, err
	}

	wire := toSiteWire(s)

	return marshalResponse(siteResponse{Site: &wire})
}

func (h *Handler) handleGetSite(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	s, err := h.Backend.GetSite(segs[1])
	if err != nil {
		return nil, err
	}

	wire := toSiteWire(s)

	return marshalResponse(siteResponse{Site: &wire})
}

func (h *Handler) handleUpdateSite(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updateSiteRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.UpdateSite(segs[1], &req)
	if err != nil {
		return nil, err
	}

	wire := toSiteWire(s)

	return marshalResponse(siteResponse{Site: &wire})
}

func (h *Handler) handleDeleteSite(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	if err := h.Backend.DeleteSite(segs[1]); err != nil {
		return nil, err
	}

	return nil, nil // void response, matches services/grafana precedent
}

func (h *Handler) handleListSites(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	f := listSitesFilter{
		cities:        q["OperatingAddressCityFilter"],
		countryCodes:  q["OperatingAddressCountryCodeFilter"],
		statesRegions: q["OperatingAddressStateOrRegionFilter"],
	}

	p := h.Backend.ListSites(f, q.Get("NextToken"), queryMaxResults(q))

	resp := listSitesResponse{NextToken: p.Next, Sites: make([]siteWire, 0, len(p.Data))}
	for _, s := range p.Data {
		resp.Sites = append(resp.Sites, toSiteWire(s))
	}

	return marshalResponse(resp)
}

func (h *Handler) handleGetSiteAddress(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	addressType := r.URL.Query().Get("AddressType")

	addr, err := h.Backend.GetSiteAddress(segs[1], addressType)
	if err != nil {
		return nil, err
	}

	return marshalResponse(getSiteAddressResponse{
		Address:     toAddressWire(addr),
		AddressType: addressType,
		SiteId:      segs[1],
	})
}

func (h *Handler) handleUpdateSiteAddress(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updateSiteAddressRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	if req.Address == nil {
		return nil, validationError("Address is required")
	}

	addr, err := h.Backend.UpdateSiteAddress(segs[1], req.AddressType, fromAddressWire(req.Address))
	if err != nil {
		return nil, err
	}

	return marshalResponse(updateSiteAddressResponse{
		Address:     toAddressWire(addr),
		AddressType: req.AddressType,
	})
}

func (h *Handler) handleUpdateSiteRackPhysicalProperties(
	_ context.Context, r *http.Request, body []byte,
) ([]byte, error) {
	segs := rawPathSegments(r)

	var req rackPhysicalPropertiesWire
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.UpdateSiteRackPhysicalProperties(segs[1], fromRackPropsWire(&req))
	if err != nil {
		return nil, err
	}

	wire := toSiteWire(s)

	return marshalResponse(siteResponse{Site: &wire})
}
