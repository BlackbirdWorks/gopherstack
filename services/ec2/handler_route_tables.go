package ec2

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleReplaceRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	destCIDR := vals.Get("DestinationCidrBlock")
	gatewayID := vals.Get("GatewayId")
	natGatewayID := vals.Get("NatGatewayId")

	if err := h.Backend.ReplaceRoute(rtID, destCIDR, gatewayID, natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ReplaceRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// registerRouteTablesOps registers the RouteTables operation handlers.
func registerRouteTablesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ReplaceRoute"] = h.handleReplaceRoute
}

// routeTablesSupportedOperations lists the operation names registered by
// registerRouteTablesOps, for GetSupportedOperations().
func routeTablesSupportedOperations() []string {
	return []string{
		"ReplaceRoute",
	}
}
