package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleModifyVpnConnection(vals url.Values, reqID string) (any, error) {
	vpnID := vals.Get("VpnConnectionId")
	vgwID := vals.Get("VpnGatewayId")
	if err := h.Backend.ModifyVpnConnection(vpnID, vgwID); err != nil {
		return nil, err
	}

	conns := h.Backend.DescribeVpnConnections([]string{vpnID})
	if len(conns) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrVpnConnectionNotFound, vpnID)
	}

	return &modifyVpnConnectionResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		VpnConnection: h.toVpnConnectionItem(conns[0]),
	}, nil
}

func (h *Handler) handleCreateVpnConnectionRoute(vals url.Values, reqID string) (any, error) {
	vpnID := vals.Get("VpnConnectionId")
	destCIDR := vals.Get("DestinationCidrBlock")

	if _, err := h.Backend.CreateVpnConnectionRoute(vpnID, destCIDR); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "CreateVpnConnectionRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDeleteVpnConnectionRoute(vals url.Values, reqID string) (any, error) {
	vpnID := vals.Get("VpnConnectionId")
	destCIDR := vals.Get("DestinationCidrBlock")
	if err := h.Backend.DeleteVpnConnectionRoute(vpnID, destCIDR); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteVpnConnectionRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// registerVpnConnectionsOps registers the VpnConnections operation handlers.
func registerVpnConnectionsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ModifyVpnConnection"] = h.handleModifyVpnConnection
	ops["CreateVpnConnectionRoute"] = h.handleCreateVpnConnectionRoute
	ops["DeleteVpnConnectionRoute"] = h.handleDeleteVpnConnectionRoute
}

// vpnConnectionsSupportedOperations lists the operation names registered by
// registerVpnConnectionsOps, for GetSupportedOperations().
func vpnConnectionsSupportedOperations() []string {
	return []string{
		"ModifyVpnConnection",
		"CreateVpnConnectionRoute",
		"DeleteVpnConnectionRoute",
	}
}
