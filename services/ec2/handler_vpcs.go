package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

type createDefaultVpcResponse struct {
	XMLName   xml.Name `xml:"CreateDefaultVpcResponse"`
	RequestID string   `xml:"requestId"`
	Vpc       struct {
		VpcID     string `xml:"vpcId"`
		CIDRBlock string `xml:"cidrBlock"`
		State     string `xml:"state"`
		IsDefault bool   `xml:"isDefault"`
	} `xml:"vpc"`
}

type modifyVpcTenancyResponse struct {
	XMLName   xml.Name `xml:"ModifyVpcTenancyResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type peeringOptionsItem struct {
	AllowDNSResolutionFromRemoteVPC            bool `xml:"allowDnsResolutionFromRemoteVpc"`
	AllowEgressFromLocalClassicLinkToRemoteVPC bool `xml:"allowEgressFromLocalClassicLinkToRemoteVpc"`
	AllowEgressFromLocalVPCToRemoteClassicLink bool `xml:"allowEgressFromLocalVpcToRemoteClassicLink"`
}

type modifyVpcPeeringConnectionOptionsResponse struct {
	XMLName                           xml.Name           `xml:"ModifyVpcPeeringConnectionOptionsResponse"`
	RequestID                         string             `xml:"requestId"`
	RequesterPeeringConnectionOptions peeringOptionsItem `xml:"requesterPeeringConnectionOptions"`
	AccepterPeeringConnectionOptions  peeringOptionsItem `xml:"accepterPeeringConnectionOptions"`
}

type addressAttributeItem struct {
	AllocationID string `xml:"allocationId"`
	PublicIP     string `xml:"publicIp"`
	DomainName   string `xml:"domainName,omitempty"`
}

func (h *Handler) handleCreateDefaultVpc(_ url.Values, reqID string) (any, error) {
	vpc, err := h.Backend.CreateDefaultVpc()
	if err != nil {
		return nil, err
	}
	resp := &createDefaultVpcResponse{RequestID: reqID}
	resp.Vpc.VpcID = vpc.ID
	resp.Vpc.CIDRBlock = vpc.CIDRBlock
	resp.Vpc.IsDefault = vpc.IsDefault
	resp.Vpc.State = stateAvailableImg

	return resp, nil
}

func (h *Handler) handleModifyVpcTenancy(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	tenancy := vals.Get("InstanceTenancy")
	if err := h.Backend.ModifyVpcTenancy(vpcID, tenancy); err != nil {
		return nil, err
	}

	return &modifyVpcTenancyResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleModifyVpcPeeringConnectionOptions(
	vals url.Values,
	reqID string,
) (any, error) {
	peeringID := vals.Get("VpcPeeringConnectionId")
	opts := PeeringConnectionOptions{
		AllowDNSResolutionFromRemoteVPC: vals.Get(
			"RequesterPeeringConnectionOptions.AllowDnsResolutionFromRemoteVpc",
		) == ec2BooleanTrue,
		AllowEgressFromLocalClassicLinkToRemoteVPC: vals.Get(
			"RequesterPeeringConnectionOptions.AllowEgressFromLocalClassicLinkToRemoteVpc",
		) == ec2BooleanTrue,
		AllowEgressFromLocalVPCToRemoteClassicLink: vals.Get(
			"RequesterPeeringConnectionOptions.AllowEgressFromLocalVpcToRemoteClassicLink",
		) == ec2BooleanTrue,
	}
	if err := h.Backend.ModifyVpcPeeringConnectionOptions(peeringID, opts); err != nil {
		return nil, err
	}
	resp := &modifyVpcPeeringConnectionOptionsResponse{RequestID: reqID}
	resp.RequesterPeeringConnectionOptions.AllowDNSResolutionFromRemoteVPC = opts.AllowDNSResolutionFromRemoteVPC
	resp.RequesterPeeringConnectionOptions.AllowEgressFromLocalClassicLinkToRemoteVPC = opts.AllowEgressFromLocalClassicLinkToRemoteVPC //nolint:lll // existing issue.
	resp.RequesterPeeringConnectionOptions.AllowEgressFromLocalVPCToRemoteClassicLink = opts.AllowEgressFromLocalVPCToRemoteClassicLink //nolint:lll // existing issue.

	return resp, nil
}

func (h *Handler) handleDisassociateVpcCidrBlock(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	if err := h.Backend.DisassociateVpcCidrBlock(assocID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateVpcCidrBlockResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyVpcAttribute(vals url.Values, reqID string) (any, error) {
	var attr string
	var valueStr string

	switch {
	case vals.Get("EnableDnsSupport.Value") != "":
		attr = attrEnableDNSSupport
		valueStr = vals.Get("EnableDnsSupport.Value")
	case vals.Get("EnableDnsHostnames.Value") != "":
		attr = attrEnableDNSHostnames
		valueStr = vals.Get("EnableDnsHostnames.Value")
	default:
		attr = attrEnableDNSSupport
		valueStr = ec2BooleanTrue
	}

	value, _ := strconv.ParseBool(valueStr)

	if err := h.Backend.ModifyVpcAttribute(vals.Get("VpcId"), attr, value); err != nil {
		return nil, err
	}

	return &genericReturnResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

type createVpcPeeringConnectionResponse struct {
	XMLName              xml.Name `xml:"CreateVpcPeeringConnectionResponse"`
	RequestID            string   `xml:"requestId"`
	VpcPeeringConnection struct {
		VpcPeeringConnectionID string `xml:"vpcPeeringConnectionId"`
		RequesterVpcID         string `xml:"requesterVpcInfo>vpcId"`
		AccepterVpcID          string `xml:"accepterVpcInfo>vpcId"`
		Status                 struct {
			Code string `xml:"code"`
		} `xml:"status"`
	} `xml:"vpcPeeringConnection"`
}

type deleteVpcPeeringConnectionResponse struct {
	XMLName   xml.Name `xml:"DeleteVpcPeeringConnectionResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type transitGatewayItem struct {
	TransitGatewayID string `xml:"transitGatewayId"`
	Description      string `xml:"description"`
	State            string `xml:"state"`
	OwnerID          string `xml:"ownerId"`
}

func (h *Handler) handleCreateVpcPeeringConnection(vals url.Values, reqID string) (any, error) {
	requesterVPCID := vals.Get("VpcId")
	accepterVPCID := vals.Get("PeerVpcId")

	pc, err := h.Backend.CreateVpcPeeringConnection(requesterVPCID, accepterVPCID)
	if err != nil {
		return nil, err
	}

	resp := &createVpcPeeringConnectionResponse{RequestID: reqID}
	resp.VpcPeeringConnection.VpcPeeringConnectionID = pc.VpcPeeringConnectionID
	resp.VpcPeeringConnection.RequesterVpcID = pc.RequesterVpcID
	resp.VpcPeeringConnection.AccepterVpcID = pc.AccepterVpcID
	resp.VpcPeeringConnection.Status.Code = pc.State

	return resp, nil
}

func (h *Handler) handleDeleteVpcPeeringConnection(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VpcPeeringConnectionId")

	if err := h.Backend.DeleteVpcPeeringConnection(id); err != nil {
		return nil, err
	}

	return &deleteVpcPeeringConnectionResponse{RequestID: reqID, Return: true}, nil
}

// registerVpcsOps registers the Vpcs operation handlers.
func registerVpcsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateDefaultVpc"] = h.handleCreateDefaultVpc
	ops["ModifyVpcTenancy"] = h.handleModifyVpcTenancy
	ops["ModifyVpcPeeringConnectionOptions"] = h.handleModifyVpcPeeringConnectionOptions
	ops["DisassociateVpcCidrBlock"] = h.handleDisassociateVpcCidrBlock
	ops["ModifyVpcAttribute"] = h.handleModifyVpcAttribute
	ops["CreateVpcPeeringConnection"] = h.handleCreateVpcPeeringConnection
	ops["DeleteVpcPeeringConnection"] = h.handleDeleteVpcPeeringConnection
}

// vpcsSupportedOperations lists the operation names registered by
// registerVpcsOps, for GetSupportedOperations().
func vpcsSupportedOperations() []string {
	return []string{
		"CreateDefaultVpc",
		"ModifyVpcTenancy",
		"ModifyVpcPeeringConnectionOptions",
		"DisassociateVpcCidrBlock",
		"ModifyVpcAttribute",
		"CreateVpcPeeringConnection",
		"DeleteVpcPeeringConnection",
	}
}
