package ec2

import (
	"encoding/xml"
	"net/url"
)

// ---- Handler registration ----

func registerVpnConcentratorOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateVpnConcentrator"] = h.handleCreateVpnConcentrator
	ops["DeleteVpnConcentrator"] = h.handleDeleteVpnConcentrator
	ops["DescribeVpnConcentrators"] = h.handleDescribeVpnConcentrators
	ops["GetActiveVpnTunnelStatus"] = h.handleGetActiveVpnTunnelStatus
	ops["ReplaceVpnTunnel"] = h.handleReplaceVpnTunnel
}

func vpnConcentratorSupportedOperations() []string {
	return []string{
		"CreateVpnConcentrator",
		"DeleteVpnConcentrator",
		"DescribeVpnConcentrators",
		"GetActiveVpnTunnelStatus",
		"ReplaceVpnTunnel",
	}
}

// ---- XML entity types ----

type vpnConcentratorItem struct {
	VpnConcentratorID          string          `xml:"vpnConcentratorId"`
	Type                       string          `xml:"type"`
	State                      string          `xml:"state"`
	TransitGatewayID           string          `xml:"transitGatewayId,omitempty"`
	TransitGatewayAttachmentID string          `xml:"transitGatewayAttachmentId,omitempty"`
	TagSet                     []simpleTagItem `xml:"tagSet>item"`
}

func vpnConcentratorToItem(vc *VpnConcentrator, tags map[string]string) vpnConcentratorItem {
	return vpnConcentratorItem{
		VpnConcentratorID:          vc.VpnConcentratorID,
		Type:                       vc.Type,
		State:                      vc.State,
		TransitGatewayID:           vc.TransitGatewayID,
		TransitGatewayAttachmentID: vc.TransitGatewayAttachmentID,
		TagSet:                     tagItemsFromMap(tags),
	}
}

type activeVpnTunnelStatusItem struct {
	IKEVersion                string `xml:"ikeVersion,omitempty"`
	Phase1EncryptionAlgorithm string `xml:"phase1EncryptionAlgorithm,omitempty"`
	Phase1IntegrityAlgorithm  string `xml:"phase1IntegrityAlgorithm,omitempty"`
	Phase2EncryptionAlgorithm string `xml:"phase2EncryptionAlgorithm,omitempty"`
	Phase2IntegrityAlgorithm  string `xml:"phase2IntegrityAlgorithm,omitempty"`
	ProvisioningStatus        string `xml:"provisioningStatus,omitempty"`
	ProvisioningStatusReason  string `xml:"provisioningStatusReason,omitempty"`
	Phase1DHGroup             int32  `xml:"phase1DHGroup,omitempty"`
	Phase2DHGroup             int32  `xml:"phase2DHGroup,omitempty"`
}

// ---- XML response types ----

type createVpnConcentratorResponse struct {
	XMLName         xml.Name            `xml:"CreateVpnConcentratorResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	RequestID       string              `xml:"requestId"`
	VpnConcentrator vpnConcentratorItem `xml:"vpnConcentrator"`
}

type deleteVpnConcentratorResponse struct {
	XMLName   xml.Name `xml:"DeleteVpnConcentratorResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type describeVpnConcentratorsResponse struct {
	XMLName          xml.Name              `xml:"DescribeVpnConcentratorsResponse"`
	Xmlns            string                `xml:"xmlns,attr"`
	RequestID        string                `xml:"requestId"`
	NextToken        string                `xml:"nextToken,omitempty"`
	VpnConcentrators []vpnConcentratorItem `xml:"vpnConcentratorSet>item"`
}

type getActiveVpnTunnelStatusResponse struct {
	XMLName               xml.Name                  `xml:"GetActiveVpnTunnelStatusResponse"`
	Xmlns                 string                    `xml:"xmlns,attr"`
	RequestID             string                    `xml:"requestId"`
	ActiveVpnTunnelStatus activeVpnTunnelStatusItem `xml:"activeVpnTunnelStatus"`
}

type replaceVpnTunnelResponse struct {
	XMLName   xml.Name `xml:"ReplaceVpnTunnelResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// ---- handlers ----

func (h *Handler) handleCreateVpnConcentrator(vals url.Values, reqID string) (any, error) {
	tags := parseTagSpecification(vals, "vpn-concentrator")

	vc, err := h.Backend.CreateVpnConcentrator(vals.Get("Type"), vals.Get("TransitGatewayId"), tags)
	if err != nil {
		return nil, err
	}

	return &createVpnConcentratorResponse{
		Xmlns:           ec2XMLNS,
		RequestID:       reqID,
		VpnConcentrator: vpnConcentratorToItem(vc, h.Backend.TagsForResource(vc.VpnConcentratorID)),
	}, nil
}

func (h *Handler) handleDeleteVpnConcentrator(vals url.Values, reqID string) (any, error) {
	if _, err := h.Backend.DeleteVpnConcentrator(vals.Get("VpnConcentratorId")); err != nil {
		return nil, err
	}

	return &deleteVpnConcentratorResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleDescribeVpnConcentrators(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpnConcentratorId")
	vcs := h.Backend.DescribeVpnConcentrators(ids)

	resp := &describeVpnConcentratorsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, vc := range vcs {
		resp.VpnConcentrators = append(
			resp.VpnConcentrators, vpnConcentratorToItem(vc, h.Backend.TagsForResource(vc.VpnConcentratorID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleGetActiveVpnTunnelStatus(vals url.Values, reqID string) (any, error) {
	status, err := h.Backend.GetActiveVpnTunnelStatus(
		vals.Get("VpnConnectionId"),
		vals.Get("VpnTunnelOutsideIpAddress"),
	)
	if err != nil {
		return nil, err
	}

	return &getActiveVpnTunnelStatusResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ActiveVpnTunnelStatus: activeVpnTunnelStatusItem{
			IKEVersion:                status.IKEVersion,
			Phase1DHGroup:             status.Phase1DHGroup,
			Phase1EncryptionAlgorithm: status.Phase1EncryptionAlgorithm,
			Phase1IntegrityAlgorithm:  status.Phase1IntegrityAlgorithm,
			Phase2DHGroup:             status.Phase2DHGroup,
			Phase2EncryptionAlgorithm: status.Phase2EncryptionAlgorithm,
			Phase2IntegrityAlgorithm:  status.Phase2IntegrityAlgorithm,
			ProvisioningStatus:        status.ProvisioningStatus,
			ProvisioningStatusReason:  status.ProvisioningStatusReason,
		},
	}, nil
}

func (h *Handler) handleReplaceVpnTunnel(vals url.Values, reqID string) (any, error) {
	ok, err := h.Backend.ReplaceVpnTunnel(vals.Get("VpnConnectionId"), vals.Get("VpnTunnelOutsideIpAddress"))
	if err != nil {
		return nil, err
	}

	return &replaceVpnTunnelResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: ok}, nil
}
