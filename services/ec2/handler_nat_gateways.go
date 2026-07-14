package ec2

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDisassociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	if err := h.Backend.DisassociateNatGatewayAddress(natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	allocationID := vals.Get("AllocationId")
	if err := h.Backend.AssociateNatGatewayAddress(natGatewayID, allocationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssignPrivateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	if err := h.Backend.AssignPrivateNatGatewayAddress(natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AssignPrivateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type unassignPrivateNatGatewayAddressResponse struct {
	XMLName             xml.Name             `xml:"UnassignPrivateNatGatewayAddressResponse"`
	Xmlns               string               `xml:"xmlns,attr"`
	RequestID           string               `xml:"requestId"`
	NatGatewayID        string               `xml:"natGatewayId,omitempty"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
}

func (h *Handler) handleUnassignPrivateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	privateIPs := parseMemberList(vals, "PrivateIpAddress")

	ngw, err := h.Backend.UnassignPrivateNatGatewayAddress(natGatewayID, privateIPs)
	if err != nil {
		return nil, err
	}

	item := toNatGatewayItem(ngw)

	return &unassignPrivateNatGatewayAddressResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NatGatewayID:        ngw.ID,
		NatGatewayAddresses: item.NatGatewayAddresses,
	}, nil
}

// ---- Image extras ----

// registerNatGatewaysOps registers the NatGateways operation handlers.
func registerNatGatewaysOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DisassociateNatGatewayAddress"] = h.handleDisassociateNatGatewayAddress
	ops["AssociateNatGatewayAddress"] = h.handleAssociateNatGatewayAddress
	ops["AssignPrivateNatGatewayAddress"] = h.handleAssignPrivateNatGatewayAddress
	ops["UnassignPrivateNatGatewayAddress"] = h.handleUnassignPrivateNatGatewayAddress
}

// natGatewaysSupportedOperations lists the operation names registered by
// registerNatGatewaysOps, for GetSupportedOperations().
func natGatewaysSupportedOperations() []string {
	return []string{
		"DisassociateNatGatewayAddress",
		"AssociateNatGatewayAddress",
		"AssignPrivateNatGatewayAddress",
		"UnassignPrivateNatGatewayAddress",
	}
}
