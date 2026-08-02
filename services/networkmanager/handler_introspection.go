package networkmanager

import (
	"context"
	"net/http"
	"sort"
)

// introspectionRoutes wires PARITY.md families T and U (6 ops).
func (h *Handler) introspectionRoutes() []route {
	return []route{
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, "network-resources"},
			op:      "GetNetworkResources",
			fn:      h.dispatchGetNetworkResources,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, "network-resource-count"},
			op:      "GetNetworkResourceCounts",
			fn:      h.dispatchGetNetworkResourceCounts,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, "network-resource-relationships"},
			op:      "GetNetworkResourceRelationships",
			fn:      h.dispatchGetNetworkResourceRelationships,
		},
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, "network-routes"},
			op:      "GetNetworkRoutes",
			fn:      h.dispatchGetNetworkRoutes,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, "network-telemetry"},
			op:      "GetNetworkTelemetry",
			fn:      h.dispatchGetNetworkTelemetry,
		},
		{
			method: http.MethodPatch,
			pattern: []string{
				segGlobalNetworks,
				paramGlobalNetworkID,
				"network-resources",
				paramResourceArn,
				"metadata",
			},
			op: "UpdateNetworkResourceMetadata",
			fn: h.dispatchUpdateNetworkResourceMetadata,
		},
	}
}

func queryFilter(q map[string][]string) networkResourceFilter {
	get := func(k string) string {
		if v, ok := q[k]; ok && len(v) > 0 {
			return v[0]
		}

		return ""
	}

	return networkResourceFilter{
		CoreNetworkID: get("coreNetworkId"),
		ResourceArn:   get("resourceArn"),
		ResourceType:  get("resourceType"),
	}
}

func (h *Handler) dispatchGetNetworkResources(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetNetworkResources(
		params["GlobalNetworkId"],
		queryFilter(q),
		queryNextToken(q),
		queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]networkResourceWire, len(p.Data))
	for i, item := range p.Data {
		out[i] = networkResourceWire{
			AccountID: h.Backend.accountID, AwsRegion: h.Backend.region, CoreNetworkID: item.CoreNetworkID,
			Definition: item.Definition, ResourceArn: item.Arn, ResourceType: item.ResourceType,
			DefinitionTimestamp: epochPtr(nowUTC()),
		}
	}

	return marshalResponse(getNetworkResourcesResponse{NetworkResources: out, NextToken: p.Next})
}

func (h *Handler) dispatchGetNetworkResourceCounts(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()
	counts := h.Backend.GetNetworkResourceCounts(params["GlobalNetworkId"], q.Get("resourceType"))

	types := make([]string, 0, len(counts))
	for t := range counts {
		types = append(types, t)
	}

	sort.Strings(types)

	out := make([]networkResourceCountWire, len(types))
	for i, t := range types {
		out[i] = networkResourceCountWire{Count: counts[t], ResourceType: t}
	}

	return marshalResponse(getNetworkResourceCountsResponse{NetworkResourceCounts: out})
}

func (h *Handler) dispatchGetNetworkResourceRelationships(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetNetworkResourceRelationships(
		params["GlobalNetworkId"], queryFilter(q), queryNextToken(q), queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	out := make([]relationshipWire, len(p.Data))
	for i, rel := range p.Data {
		out[i] = relationshipWire(rel)
	}

	return marshalResponse(getNetworkResourceRelationshipsResponse{Relationships: out, NextToken: p.Next})
}

func (h *Handler) dispatchGetNetworkRoutes(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req getNetworkRoutesReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	tgwArn := ""

	var hasSegmentEdge bool

	var segmentEdge *coreNetworkSegmentEdgeIdentifierWire

	if req.RouteTableIdentifier != nil {
		tgwArn = req.RouteTableIdentifier.TransitGatewayRouteTableArn
		if req.RouteTableIdentifier.CoreNetworkSegmentEdge != nil {
			hasSegmentEdge = true
			segmentEdge = req.RouteTableIdentifier.CoreNetworkSegmentEdge
		}
	}

	routeTableArn, routeTableType, err := h.Backend.GetNetworkRoutes(params["GlobalNetworkId"], tgwArn, hasSegmentEdge)
	if err != nil {
		return nil, err
	}

	return marshalResponse(getNetworkRoutesResponse{
		CoreNetworkSegmentEdge: segmentEdge, NetworkRoutes: []struct{}{}, RouteTableArn: routeTableArn,
		RouteTableType: routeTableType,
	})
}

func (h *Handler) dispatchGetNetworkTelemetry(
	_ context.Context,
	r *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	q := r.URL.Query()

	p, err := h.Backend.GetNetworkTelemetry(
		params["GlobalNetworkId"],
		queryFilter(q),
		queryNextToken(q),
		queryMaxResults(q),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(getNetworkTelemetryResponse{NetworkTelemetry: p.Data, NextToken: p.Next})
}

func (h *Handler) dispatchUpdateNetworkResourceMetadata(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateNetworkResourceMetadataReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	resourceArn := params["ResourceArn"]

	metadata, err := h.Backend.UpdateNetworkResourceMetadata(params["GlobalNetworkId"], resourceArn, req.Metadata)
	if err != nil {
		return nil, err
	}

	return marshalResponse(updateNetworkResourceMetadataResponse{Metadata: metadata, ResourceArn: resourceArn})
}
