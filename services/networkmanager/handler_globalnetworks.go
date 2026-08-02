package networkmanager

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// globalNetworksRoutes wires PARITY.md families A-F (23 ops): the Global
// Networks base container hierarchy plus Connection. Split across one
// helper per sub-family (rather than one long literal) to keep this
// function's own length under funlen's limit.
func (h *Handler) globalNetworksRoutes() []route {
	return concatRoutes(
		h.globalNetworkCoreRoutes(),
		h.siteRoutes(),
		h.deviceRoutes(),
		h.linkRoutes(),
		h.linkAssociationRoutes(),
		h.connectionRoutes(),
	)
}

func (h *Handler) globalNetworkCoreRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks},
			op:      "CreateGlobalNetwork",
			fn:      h.dispatchCreateGlobalNetwork,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID},
			op:      "UpdateGlobalNetwork",
			fn:      h.dispatchUpdateGlobalNetwork,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID},
			op:      "DeleteGlobalNetwork",
			fn:      h.dispatchDeleteGlobalNetwork,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks},
			op:      "DescribeGlobalNetworks",
			fn:      h.dispatchDescribeGlobalNetworks,
		},
	}
}

func (h *Handler) siteRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segSites},
			op:      "CreateSite",
			fn:      h.dispatchCreateSite,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segSites, ":SiteId"},
			op:      "UpdateSite",
			fn:      h.dispatchUpdateSite,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segSites, ":SiteId"},
			op:      "DeleteSite",
			fn:      h.dispatchDeleteSite,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segSites},
			op:      "GetSites",
			fn:      h.dispatchGetSites,
		},
	}
}

func (h *Handler) deviceRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segDevices},
			op:      "CreateDevice",
			fn:      h.dispatchCreateDevice,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segDevices, ":DeviceId"},
			op:      "UpdateDevice",
			fn:      h.dispatchUpdateDevice,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segDevices, ":DeviceId"},
			op:      "DeleteDevice",
			fn:      h.dispatchDeleteDevice,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segDevices},
			op:      "GetDevices",
			fn:      h.dispatchGetDevices,
		},
	}
}

func (h *Handler) linkRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segLinks},
			op:      "CreateLink",
			fn:      h.dispatchCreateLink,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segLinks, ":LinkId"},
			op:      "UpdateLink",
			fn:      h.dispatchUpdateLink,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segLinks, ":LinkId"},
			op:      "DeleteLink",
			fn:      h.dispatchDeleteLink,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segLinks},
			op:      "GetLinks",
			fn:      h.dispatchGetLinks,
		},
	}
}

func (h *Handler) linkAssociationRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segLinkAssociations},
			op:      "AssociateLink",
			fn:      h.dispatchAssociateLink,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segLinkAssociations},
			op:      "DisassociateLink",
			fn:      h.dispatchDisassociateLink,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segLinkAssociations},
			op:      "GetLinkAssociations",
			fn:      h.dispatchGetLinkAssociations,
		},
	}
}

func (h *Handler) connectionRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segConnections},
			op:      "CreateConnection",
			fn:      h.dispatchCreateConnection,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segConnections, ":ConnectionId"},
			op:      "UpdateConnection",
			fn:      h.dispatchUpdateConnection,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segConnections, ":ConnectionId"},
			op:      "DeleteConnection",
			fn:      h.dispatchDeleteConnection,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, segConnections},
			op:      "GetConnections",
			fn:      h.dispatchGetConnections,
		},
	}
}

// ---- GlobalNetwork ----

func (h *Handler) dispatchCreateGlobalNetwork(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createGlobalNetworkReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	g := h.Backend.CreateGlobalNetwork(req.Description, tags.MapFromKV(req.Tags))

	return marshalResponse(globalNetworkEnvelope{GlobalNetwork: toGlobalNetworkWire(g)})
}

func (h *Handler) dispatchUpdateGlobalNetwork(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateGlobalNetworkReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	g, err := h.Backend.UpdateGlobalNetwork(params["GlobalNetworkId"], req.Description)
	if err != nil {
		return nil, err
	}

	return marshalResponse(globalNetworkEnvelope{GlobalNetwork: toGlobalNetworkWire(g)})
}

func (h *Handler) dispatchDeleteGlobalNetwork(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	g, err := h.Backend.DeleteGlobalNetwork(params["GlobalNetworkId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(globalNetworkEnvelope{GlobalNetwork: toGlobalNetworkWire(g)})
}

func (h *Handler) dispatchDescribeGlobalNetworks(
	_ context.Context,
	r *http.Request,
	_ routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.DescribeGlobalNetworks(q["globalNetworkIds"], queryNextToken(q), queryMaxResults(q))
	if err != nil {
		return nil, err
	}

	out := make([]globalNetworkWire, len(p.Data))
	for i, g := range p.Data {
		out[i] = *toGlobalNetworkWire(g)
	}

	return marshalResponse(describeGlobalNetworksResponse{GlobalNetworks: out, NextToken: p.Next})
}

// ---- Site ----

func (h *Handler) dispatchCreateSite(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req createSiteReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.CreateSite(
		params["GlobalNetworkId"], req.Description, fromLocationWire(req.Location), tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(siteEnvelope{Site: toSiteWire(s)})
}

func (h *Handler) dispatchUpdateSite(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateSiteReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	s, err := h.Backend.UpdateSite(
		params["GlobalNetworkId"], params["SiteId"], req.Description, fromLocationWire(req.Location),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(siteEnvelope{Site: toSiteWire(s)})
}

func (h *Handler) dispatchDeleteSite(_ context.Context, _ *http.Request, params routeParams, _ []byte) ([]byte, error) {
	s, err := h.Backend.DeleteSite(params["GlobalNetworkId"], params["SiteId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(siteEnvelope{Site: toSiteWire(s)})
}

func (h *Handler) dispatchGetSites(_ context.Context, r *http.Request, params routeParams, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetSites(params["GlobalNetworkId"], q["siteIds"], queryNextToken(q), queryMaxResults(q))
	if err != nil {
		return nil, err
	}

	out := make([]siteWire, len(p.Data))
	for i, s := range p.Data {
		out[i] = *toSiteWire(s)
	}

	return marshalResponse(getSitesResponse{Sites: out, NextToken: p.Next})
}

// ---- Device ----

func (h *Handler) dispatchCreateDevice(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req createDeviceReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	d, err := h.Backend.CreateDevice(
		params["GlobalNetworkId"], fromAWSLocationWire(req.AWSLocation), fromLocationWire(req.Location),
		req.Description, req.Model, req.SerialNumber, req.SiteID, req.Type, req.Vendor, tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(deviceEnvelope{Device: toDeviceWire(d)})
}

func (h *Handler) dispatchUpdateDevice(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateDeviceReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	d, err := h.Backend.UpdateDevice(
		params["GlobalNetworkId"],
		params["DeviceId"],
		fromAWSLocationWire(req.AWSLocation),
		fromLocationWire(req.Location),
		req.Description,
		req.Model,
		req.SerialNumber,
		req.SiteID,
		req.Type,
		req.Vendor,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(deviceEnvelope{Device: toDeviceWire(d)})
}

func (h *Handler) dispatchDeleteDevice(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	d, err := h.Backend.DeleteDevice(params["GlobalNetworkId"], params["DeviceId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(deviceEnvelope{Device: toDeviceWire(d)})
}

func (h *Handler) dispatchGetDevices(_ context.Context, r *http.Request, params routeParams, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetDevices(
		params["GlobalNetworkId"],
		q["deviceIds"],
		q.Get("siteId"),
		queryNextToken(q),
		queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]deviceWire, len(p.Data))
	for i, d := range p.Data {
		out[i] = *toDeviceWire(d)
	}

	return marshalResponse(getDevicesResponse{Devices: out, NextToken: p.Next})
}

// ---- Link ----

func (h *Handler) dispatchCreateLink(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req createLinkReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	l, err := h.Backend.CreateLink(
		params["GlobalNetworkId"], req.SiteID, fromBandwidthWire(req.Bandwidth),
		req.Description, req.Provider, req.Type, tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(linkEnvelope{Link: toLinkWire(l)})
}

func (h *Handler) dispatchUpdateLink(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateLinkReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	l, err := h.Backend.UpdateLink(
		params["GlobalNetworkId"],
		params["LinkId"],
		fromBandwidthWire(req.Bandwidth),
		req.Description,
		req.Provider,
		req.Type,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(linkEnvelope{Link: toLinkWire(l)})
}

func (h *Handler) dispatchDeleteLink(_ context.Context, _ *http.Request, params routeParams, _ []byte) ([]byte, error) {
	l, err := h.Backend.DeleteLink(params["GlobalNetworkId"], params["LinkId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(linkEnvelope{Link: toLinkWire(l)})
}

func (h *Handler) dispatchGetLinks(_ context.Context, r *http.Request, params routeParams, _ []byte) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetLinks(
		params["GlobalNetworkId"], q["linkIds"], q.Get("provider"), q.Get("siteId"), q.Get("type"),
		queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]linkWire, len(p.Data))
	for i, l := range p.Data {
		out[i] = *toLinkWire(l)
	}

	return marshalResponse(getLinksResponse{Links: out, NextToken: p.Next})
}

// ---- Link Association ----

func (h *Handler) dispatchAssociateLink(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req linkAssociationReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.AssociateLink(params["GlobalNetworkId"], req.DeviceID, req.LinkID)
	if err != nil {
		return nil, err
	}

	return marshalResponse(linkAssociationEnvelope{LinkAssociation: toLinkAssociationWire(a)})
}

// dispatchDisassociateLink reads DeviceID/LinkID from the query string, NOT
// the JSON body -- confirmed by direct SDK read
// (awsRestjson1_serializeOpHttpBindingsDisassociateLinkInput uses
// encoder.SetQuery, not a document body) despite PARITY.md's own summary
// table describing this op ambiguously as "body/query fields". A round-trip
// test against the real SDK client caught this: decoding an empty body
// silently produced empty DeviceID/LinkID, which a unit test asserting
// against the handler directly would never have exposed.
func (h *Handler) dispatchDisassociateLink(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	a, err := h.Backend.DisassociateLink(params["GlobalNetworkId"], q.Get("deviceId"), q.Get("linkId"))
	if err != nil {
		return nil, err
	}

	return marshalResponse(linkAssociationEnvelope{LinkAssociation: toLinkAssociationWire(a)})
}

func (h *Handler) dispatchGetLinkAssociations(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetLinkAssociations(
		params["GlobalNetworkId"], q.Get("deviceId"), q.Get("linkId"), queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]linkAssociationWire, len(p.Data))
	for i, a := range p.Data {
		out[i] = *toLinkAssociationWire(a)
	}

	return marshalResponse(getLinkAssociationsResponse{LinkAssociations: out, NextToken: p.Next})
}

// ---- Connection ----

func (h *Handler) dispatchCreateConnection(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req createConnectionReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.CreateConnection(
		params["GlobalNetworkId"],
		req.ConnectedDeviceID,
		req.DeviceID,
		req.ConnectedLinkID,
		req.Description,
		req.LinkID,
		tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectionEnvelope{Connection: toConnectionWire(c)})
}

func (h *Handler) dispatchUpdateConnection(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateConnectionReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	c, err := h.Backend.UpdateConnection(
		params["GlobalNetworkId"], params["ConnectionId"], req.ConnectedLinkID, req.Description, req.LinkID,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectionEnvelope{Connection: toConnectionWire(c)})
}

func (h *Handler) dispatchDeleteConnection(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	c, err := h.Backend.DeleteConnection(params["GlobalNetworkId"], params["ConnectionId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectionEnvelope{Connection: toConnectionWire(c)})
}

func (h *Handler) dispatchGetConnections(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetConnections(
		params["GlobalNetworkId"],
		q["connectionIds"],
		q.Get("deviceId"),
		queryNextToken(q),
		queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]connectionWire, len(p.Data))
	for i, c := range p.Data {
		out[i] = *toConnectionWire(c)
	}

	return marshalResponse(getConnectionsResponse{Connections: out, NextToken: p.Next})
}
