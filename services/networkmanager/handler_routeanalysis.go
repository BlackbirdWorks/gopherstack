package networkmanager

import (
	"context"
	"net/http"
)

// routeAnalysisRoutes wires PARITY.md family S (2 ops).
func (h *Handler) routeAnalysisRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, "route-analyses"},
			op:      "StartRouteAnalysis",
			fn:      h.dispatchStartRouteAnalysis,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segGlobalNetworks, paramGlobalNetworkID, "route-analyses", ":RouteAnalysisId"},
			op:      "GetRouteAnalysis",
			fn:      h.dispatchGetRouteAnalysis,
		},
	}
}

func fromRouteAnalysisEndpointSpec(s *routeAnalysisEndpointSpecWire) *RouteAnalysisEndpoint {
	if s == nil {
		return nil
	}

	return &RouteAnalysisEndpoint{IPAddress: s.IPAddress, TransitGatewayAttachmentArn: s.TransitGatewayAttachmentArn}
}

func (h *Handler) dispatchStartRouteAnalysis(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req startRouteAnalysisReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	r, err := h.Backend.StartRouteAnalysis(
		params["GlobalNetworkId"],
		fromRouteAnalysisEndpointSpec(req.Source),
		fromRouteAnalysisEndpointSpec(req.Destination),
		req.IncludeReturnPath,
		req.UseMiddleboxes,
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(routeAnalysisEnvelope{RouteAnalysis: toRouteAnalysisWire(r)})
}

func (h *Handler) dispatchGetRouteAnalysis(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	r, err := h.Backend.GetRouteAnalysis(params["GlobalNetworkId"], params["RouteAnalysisId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(routeAnalysisEnvelope{RouteAnalysis: toRouteAnalysisWire(r)})
}
