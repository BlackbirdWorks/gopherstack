package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
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

// ---- VPN Connection handlers ----

func (h *Handler) handleCreateVpnConnection(vals url.Values, reqID string) (any, error) {
	conn, err := h.Backend.CreateVpnConnection(
		vals.Get("Type"),
		vals.Get("CustomerGatewayId"),
		vals.Get("VpnGatewayId"),
	)
	if err != nil {
		return nil, err
	}

	return &createVpnConnectionResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		VpnConnection: h.toVpnConnectionItem(conn),
	}, nil
}

func (h *Handler) handleDescribeVpnConnections(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpnConnectionId")
	conns := h.Backend.DescribeVpnConnections(ids)

	resp := &describeVpnConnectionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, conn := range conns {
		resp.VpnConnectionSet.Items = append(resp.VpnConnectionSet.Items, h.toVpnConnectionItem(conn))
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpnConnection(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteVpnConnection(vals.Get("VpnConnectionId")); err != nil {
		return nil, err
	}

	return &deleteVpnConnectionResponse{RequestID: reqID, Return: true}, nil
}

// ---- VPN Connection tunnel/option modification handlers ----

// handleModifyVpnConnectionOptions handles ModifyVpnConnectionOptions. Real AWS accepts only
// the local/remote IPv4 (and IPv6) network CIDRs here — StaticRoutesOnly is fixed at
// CreateVpnConnection time and is not one of this action's parameters, so it is never
// overridden from the request (nil below leaves it unchanged).
func (h *Handler) handleModifyVpnConnectionOptions(vals url.Values, reqID string) (any, error) {
	conn, err := h.Backend.ModifyVpnConnectionOptions(
		vals.Get("VpnConnectionId"),
		vals.Get("LocalIpv4NetworkCidr"),
		vals.Get("RemoteIpv4NetworkCidr"),
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &modifyVpnConnectionOptionsResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		VpnConnection: h.toVpnConnectionItem(conn),
	}, nil
}

// parseInt32Param parses a query parameter as an int32, returning 0 if absent or malformed.
func parseInt32Param(vals url.Values, key string) int32 {
	v := vals.Get(key)
	if v == "" {
		return 0
	}

	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}

// parseIKEVersionValues parses the indexed "TunnelOptions.IKEVersions.N.Value" query parameters
// ModifyVpnTunnelOptions accepts.
func parseIKEVersionValues(vals url.Values) []string {
	var out []string

	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("TunnelOptions.IKEVersions.%d.Value", i))
		if v == "" {
			break
		}

		out = append(out, v)
	}

	return out
}

func (h *Handler) handleModifyVpnTunnelOptions(vals url.Values, reqID string) (any, error) {
	vpnID := vals.Get("VpnConnectionId")
	outsideIP := vals.Get("VpnTunnelOutsideIpAddress")

	opts := VpnTunnelOptionsModify{
		TunnelInsideCIDR:       vals.Get("TunnelOptions.TunnelInsideCidr"),
		PreSharedKey:           vals.Get("TunnelOptions.PreSharedKey"),
		Phase1LifetimeSeconds:  parseInt32Param(vals, "TunnelOptions.Phase1LifetimeSeconds"),
		Phase2LifetimeSeconds:  parseInt32Param(vals, "TunnelOptions.Phase2LifetimeSeconds"),
		RekeyMarginTimeSeconds: parseInt32Param(vals, "TunnelOptions.RekeyMarginTimeSeconds"),
		DPDTimeoutSeconds:      parseInt32Param(vals, "TunnelOptions.DPDTimeoutSeconds"),
		DPDTimeoutAction:       vals.Get("TunnelOptions.DPDTimeoutAction"),
		StartupAction:          vals.Get("TunnelOptions.StartupAction"),
		IKEVersions:            parseIKEVersionValues(vals),
	}

	conn, err := h.Backend.ModifyVpnTunnelOptions(vpnID, outsideIP, opts)
	if err != nil {
		return nil, err
	}

	return &modifyVpnTunnelOptionsResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		VpnConnection: h.toVpnConnectionItem(conn),
	}, nil
}

func (h *Handler) handleModifyVpnTunnelCertificate(vals url.Values, reqID string) (any, error) {
	conn, err := h.Backend.ModifyVpnTunnelCertificate(
		vals.Get("VpnConnectionId"),
		vals.Get("VpnTunnelOutsideIpAddress"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyVpnTunnelCertificateResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		VpnConnection: h.toVpnConnectionItem(conn),
	}, nil
}

func (h *Handler) handleGetVpnConnectionDeviceTypes(_ url.Values, reqID string) (any, error) {
	types := h.Backend.GetVpnConnectionDeviceTypes()

	resp := &getVpnConnectionDeviceTypesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, t := range types {
		resp.VpnConnectionDeviceTypeSet.Items = append(
			resp.VpnConnectionDeviceTypeSet.Items,
			vpnConnectionDeviceTypeItem(t),
		)
	}

	return resp, nil
}

func (h *Handler) handleGetVpnConnectionDeviceSampleConfiguration(vals url.Values, reqID string) (any, error) {
	config, err := h.Backend.GetVpnConnectionDeviceSampleConfiguration(
		vals.Get("VpnConnectionId"),
		vals.Get("VpnConnectionDeviceTypeId"),
		vals.Get("InternetKeyExchangeVersion"),
	)
	if err != nil {
		return nil, err
	}

	return &getVpnConnectionDeviceSampleConfigurationResponse{
		Xmlns:                                  ec2XMLNS,
		RequestID:                              reqID,
		VpnConnectionDeviceSampleConfiguration: config,
	}, nil
}

func (h *Handler) handleGetVpnTunnelReplacementStatus(vals url.Values, reqID string) (any, error) {
	status, err := h.Backend.GetVpnTunnelReplacementStatus(
		vals.Get("VpnConnectionId"),
		vals.Get("VpnTunnelOutsideIpAddress"),
	)
	if err != nil {
		return nil, err
	}

	return &getVpnTunnelReplacementStatusResponse{
		Xmlns:                     ec2XMLNS,
		RequestID:                 reqID,
		VpnConnectionID:           status.VpnConnectionID,
		TransitGatewayID:          status.TransitGatewayID,
		VpnGatewayID:              status.VpnGatewayID,
		CustomerGatewayID:         status.CustomerGatewayID,
		VpnTunnelOutsideIPAddress: status.VpnTunnelOutsideIPAddress,
		MaintenanceDetails: vpnTunnelMaintenanceDetailsItem{
			PendingMaintenance: status.MaintenanceDetails.PendingMaintenance,
		},
	}, nil
}
