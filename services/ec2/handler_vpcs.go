package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
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

type transitGatewayOptionsItem struct {
	AutoAcceptSharedAttachments     string   `xml:"autoAcceptSharedAttachments,omitempty"`
	DefaultRouteTableAssociation    string   `xml:"defaultRouteTableAssociation,omitempty"`
	DefaultRouteTablePropagation    string   `xml:"defaultRouteTablePropagation,omitempty"`
	DNSSupport                      string   `xml:"dnsSupport,omitempty"`
	MulticastSupport                string   `xml:"multicastSupport,omitempty"`
	SecurityGroupReferencingSupport string   `xml:"securityGroupReferencingSupport,omitempty"`
	VpnEcmpSupport                  string   `xml:"vpnEcmpSupport,omitempty"`
	TransitGatewayCidrBlocks        []string `xml:"transitGatewayCidrBlocks>item,omitempty"`
	AmazonSideAsn                   int64    `xml:"amazonSideAsn,omitempty"`
}

type transitGatewayItem struct {
	CreationTime      string                    `xml:"creationTime,omitempty"`
	TransitGatewayID  string                    `xml:"transitGatewayId"`
	TransitGatewayArn string                    `xml:"transitGatewayArn,omitempty"`
	Description       string                    `xml:"description"`
	State             string                    `xml:"state"`
	OwnerID           string                    `xml:"ownerId"`
	TagSet            []simpleTagItem           `xml:"tagSet>item"`
	Options           transitGatewayOptionsItem `xml:"options"`
}

// toTransitGatewayItem converts a backend TransitGateway plus its tags (read
// separately via the shared tag store) into the wire item.
func toTransitGatewayItem(tgw *TransitGateway, tags map[string]string) transitGatewayItem {
	item := transitGatewayItem{
		TransitGatewayID:  tgw.ID,
		TransitGatewayArn: tgw.Arn,
		Description:       tgw.Description,
		State:             tgw.State,
		OwnerID:           tgw.OwnerID,
		TagSet:            tagItemsFromMap(tags),
		Options: transitGatewayOptionsItem{
			AmazonSideAsn:                   tgw.Options.AmazonSideAsn,
			AutoAcceptSharedAttachments:     tgw.Options.AutoAcceptSharedAttachments,
			DefaultRouteTableAssociation:    tgw.Options.DefaultRouteTableAssociation,
			DefaultRouteTablePropagation:    tgw.Options.DefaultRouteTablePropagation,
			DNSSupport:                      tgw.Options.DNSSupport,
			MulticastSupport:                tgw.Options.MulticastSupport,
			SecurityGroupReferencingSupport: tgw.Options.SecurityGroupReferencingSupport,
			VpnEcmpSupport:                  tgw.Options.VpnEcmpSupport,
			TransitGatewayCidrBlocks:        tgw.Options.TransitGatewayCidrBlocks,
		},
	}
	if !tgw.CreationTime.IsZero() {
		item.CreationTime = tgw.CreationTime.Format(time.RFC3339)
	}

	return item
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

func (h *Handler) handleDescribeVpcs(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpcId")
	vpcs := h.Backend.DescribeVpcs(ids)

	filters := parseEC2Filters(vals)
	vpcs = applyVPCFilters(vpcs, filters, h.Backend)

	items := make([]vpcItem, 0, len(vpcs))
	for _, v := range vpcs {
		items = append(items, toVPCItem(v, h.Backend.TagsForResource(v.ID)))
	}

	return &describeVpcsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VpcSet:    vpcItemSet{Items: items},
	}, nil
}

type describeVpcAttributeResponse struct {
	XMLName   xml.Name `xml:"DescribeVpcAttributeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	VpcID     string   `xml:"vpcId"`
	// Attribute has no XML tag; encoding/xml uses the namedBoolAttr.XMLName field (set at runtime)
	// to produce a dynamic element name such as <enableDnsHostnames> or <enableDnsSupport>.
	Attribute namedBoolAttr
}

// namedBoolAttr is a boolean attribute element whose XML element name is set dynamically.
type namedBoolAttr struct {
	XMLName xml.Name `json:"xmlName"`
	Value   string   `json:"value,omitempty" xml:"value"`
}

func (h *Handler) handleDescribeVpcAttribute(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	attr := vals.Get("Attribute")

	attrValue := vpcAttributeValue(h.Backend.DescribeVpcs([]string{vpcID}), attr)

	return &describeVpcAttributeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VpcID:     vpcID,
		Attribute: namedBoolAttr{XMLName: xml.Name{Local: attr}, Value: attrValue},
	}, nil
}

// vpcAttributeValue reads the persisted boolean value for a VPC attribute.
// enableDnsSupport defaults to true (AWS default); all others default to false.
func vpcAttributeValue(vpcs []*VPC, attr string) string {
	if len(vpcs) == 0 {
		if attr == attrEnableDNSSupport {
			return ec2BooleanTrue
		}

		return ec2BooleanFalse
	}

	vpc := vpcs[0]
	if v, ok := vpc.Attributes[attr]; ok {
		if v {
			return ec2BooleanTrue
		}

		return ec2BooleanFalse
	}

	if attr == attrEnableDNSSupport {
		return ec2BooleanTrue
	}

	return ec2BooleanFalse
}

func (h *Handler) handleCreateVpc(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("CidrBlock")

	v, err := h.Backend.CreateVpc(cidr)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, resourceTypeVPC)
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{v.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createVpcResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Vpc:       toVPCItem(v, tags),
	}, nil
}

func (h *Handler) handleDeleteVpc(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VpcId")
	if id == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteVpc(id); err != nil {
		return nil, err
	}

	return &deleteVpcResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func toVPCItem(v *VPC, tags map[string]string) vpcItem {
	isDefault := ec2BooleanFalse
	if v.IsDefault {
		isDefault = ec2BooleanTrue
	}

	return vpcItem{
		VpcID:     v.ID,
		CIDRBlock: v.CIDRBlock,
		IsDefault: isDefault,
		State:     stateAvailable,
		TagSet:    tagItemsFromMap(tags),
	}
}

type vpcItem struct {
	VpcID     string          `xml:"vpcId"`
	CIDRBlock string          `xml:"cidrBlock"`
	IsDefault string          `xml:"isDefault"`
	State     string          `xml:"state"`
	TagSet    []simpleTagItem `xml:"tagSet>item"`
}

type vpcItemSet struct {
	Items []vpcItem `xml:"item"`
}

type describeVpcsResponse struct {
	XMLName   xml.Name   `xml:"DescribeVpcsResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	VpcSet    vpcItemSet `xml:"vpcSet"`
}

type createVpcResponse struct {
	XMLName   xml.Name `xml:"CreateVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Vpc       vpcItem  `xml:"vpc"`
}

type deleteVpcResponse struct {
	XMLName   xml.Name `xml:"DeleteVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleRejectVpcPeeringConnection(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.RejectVpcPeeringConnection(vals.Get("VpcPeeringConnectionId")); err != nil {
		return nil, err
	}

	return &rejectVpcPeeringConnectionResponse{RequestID: reqID, Return: true}, nil
}
