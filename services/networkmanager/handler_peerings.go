package networkmanager

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// peeringsRoutes wires PARITY.md family R (4 ops).
func (h *Handler) peeringsRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{"transit-gateway-peerings"},
			op:      "CreateTransitGatewayPeering",
			fn:      h.dispatchCreateTransitGatewayPeering,
		},
		{
			method:  http.MethodGet,
			pattern: []string{"transit-gateway-peerings", paramPeeringID},
			op:      "GetTransitGatewayPeering",
			fn:      h.dispatchGetTransitGatewayPeering,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{"peerings", paramPeeringID},
			op:      "DeletePeering",
			fn:      h.dispatchDeletePeering,
		},
		{method: http.MethodGet, pattern: []string{"peerings"}, op: "ListPeerings", fn: h.dispatchListPeerings},
	}
}

func (h *Handler) dispatchCreateTransitGatewayPeering(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createTransitGatewayPeeringReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	p, err := h.Backend.CreateTransitGatewayPeering(req.CoreNetworkID, req.TransitGatewayArn, tags.MapFromKV(req.Tags))
	if err != nil {
		return nil, err
	}

	return marshalResponse(transitGatewayPeeringEnvelope{TransitGatewayPeering: toTransitGatewayPeeringWire(p)})
}

func (h *Handler) dispatchGetTransitGatewayPeering(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	p, err := h.Backend.GetTransitGatewayPeering(params["PeeringId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(transitGatewayPeeringEnvelope{TransitGatewayPeering: toTransitGatewayPeeringWire(p)})
}

func (h *Handler) dispatchDeletePeering(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	p, err := h.Backend.DeletePeering(params["PeeringId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(peeringEnvelope{Peering: toPeeringWire(p)})
}

func (h *Handler) dispatchListPeerings(_ context.Context, r *http.Request, _ routeParams, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	p := h.Backend.ListPeerings(
		q.Get("coreNetworkId"), q.Get("edgeLocation"), q.Get("peeringType"), q.Get("state"),
		queryNextToken(q), queryMaxResults(q),
	)

	out := make([]peeringWire, len(p.Data))
	for i, v := range p.Data {
		out[i] = *toPeeringWire(v)
	}

	return marshalResponse(listPeeringsResponse{Peerings: out, NextToken: p.Next})
}
