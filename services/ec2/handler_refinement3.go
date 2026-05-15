package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// ---- Handler registration ----

func registerRefinement3Ops(h *Handler, ops map[string]ec2ActionFn) {
	ops["ReplaceNetworkAclEntry"] = h.handleReplaceNetworkACLEntry
	ops["ReplaceNetworkAclAssociation"] = h.handleReplaceNetworkACLAssociation
	ops["DescribeVpcEndpointServices"] = h.handleDescribeVpcEndpointServices
	ops["ExportKeyPair"] = h.handleExportKeyPair
	ops["DescribeInstanceTypeOfferings"] = h.handleDescribeInstanceTypeOfferings
	ops["CreateVpcPeeringConnection"] = h.handleCreateVpcPeeringConnection
	ops["DeleteVpcPeeringConnection"] = h.handleDeleteVpcPeeringConnection
	ops["DescribeTransitGateways"] = h.handleDescribeTransitGateways
	ops["CreateTransitGateway"] = h.handleCreateTransitGateway
	ops["DeleteTransitGateway"] = h.handleDeleteTransitGateway
}

func refinement3SupportedOperations() []string {
	return []string{
		"ReplaceNetworkAclEntry",
		"ReplaceNetworkAclAssociation",
		"DescribeVpcEndpointServices",
		"ExportKeyPair",
		"DescribeInstanceTypeOfferings",
		"CreateVpcPeeringConnection",
		"DeleteVpcPeeringConnection",
		"DescribeTransitGateways",
		"CreateTransitGateway",
		"DeleteTransitGateway",
	}
}

// ---- XML response types ----

type replaceNetworkACLEntryResponse struct {
	XMLName   xml.Name `xml:"ReplaceNetworkAclEntryResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type replaceNetworkACLAssociationResponse struct {
	XMLName          xml.Name `xml:"ReplaceNetworkAclAssociationResponse"`
	RequestID        string   `xml:"requestId"`
	NewAssociationID string   `xml:"newAssociationId"`
}

type describeVpcEndpointServicesResponse struct {
	XMLName      xml.Name `xml:"DescribeVpcEndpointServicesResponse"`
	RequestID    string   `xml:"requestId"`
	ServiceNames struct {
		Items []serviceNameItem `xml:"item"`
	} `xml:"serviceNameSet"`
}

type serviceNameItem struct {
	ServiceName string `xml:"serviceName"`
}

type exportKeyPairResponse struct {
	XMLName           xml.Name `xml:"ExportKeyPairResponse"`
	RequestID         string   `xml:"requestId"`
	KeyName           string   `xml:"keyName"`
	PublicKeyMaterial string   `xml:"publicKeyMaterial"`
}

type instanceTypeOfferingItem struct {
	InstanceType string `xml:"instanceType"`
	Location     string `xml:"location"`
	LocationType string `xml:"locationType"`
}

type describeInstanceTypeOfferingsResponse struct {
	XMLName                 xml.Name `xml:"DescribeInstanceTypeOfferingsResponse"`
	RequestID               string   `xml:"requestId"`
	InstanceTypeOfferingSet struct {
		Items []instanceTypeOfferingItem `xml:"item"`
	} `xml:"instanceTypeOfferingSet"`
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

type describeTransitGatewaysResponse struct {
	XMLName           xml.Name `xml:"DescribeTransitGatewaysResponse"`
	RequestID         string   `xml:"requestId"`
	TransitGatewaySet struct {
		Items []transitGatewayItem `xml:"item"`
	} `xml:"transitGatewaySet"`
}

type createTransitGatewayResponse struct {
	XMLName        xml.Name           `xml:"CreateTransitGatewayResponse"`
	RequestID      string             `xml:"requestId"`
	TransitGateway transitGatewayItem `xml:"transitGateway"`
}

type deleteTransitGatewayResponse struct {
	XMLName   xml.Name `xml:"DeleteTransitGatewayResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// ---- Handler implementations ----

func (h *Handler) handleReplaceNetworkACLEntry(vals url.Values, reqID string) (any, error) {
	aclID := vals.Get("NetworkAclId")
	ruleNumber, _ := strconv.Atoi(vals.Get("RuleNumber"))
	protocol := vals.Get("Protocol")
	action := vals.Get("RuleAction")
	cidr := vals.Get("CidrBlock")
	if cidr == "" {
		cidr = vals.Get("Ipv6CidrBlock")
	}
	egress := vals.Get("Egress") == ec2BooleanTrue
	fromPort, _ := strconv.Atoi(vals.Get("PortRange.From"))
	toPort, _ := strconv.Atoi(vals.Get("PortRange.To"))

	if err := h.Backend.ReplaceNetworkACLEntry(
		aclID, ruleNumber, protocol, action, cidr, egress, fromPort, toPort,
	); err != nil {
		return nil, err
	}

	return &replaceNetworkACLEntryResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleReplaceNetworkACLAssociation(vals url.Values, reqID string) (any, error) {
	aclID := vals.Get("NetworkAclId")
	subnetID := vals.Get(
		"AssociationId",
	) // AWS sends the old assocID; we use SubnetId as a simplification
	if subnetID == "" {
		subnetID = vals.Get("SubnetId")
	}

	newAssocID, err := h.Backend.ReplaceNetworkACLAssociation(aclID, subnetID)
	if err != nil {
		return nil, err
	}

	return &replaceNetworkACLAssociationResponse{
		RequestID:        reqID,
		NewAssociationID: newAssocID,
	}, nil
}

func (h *Handler) handleDescribeVpcEndpointServices(_ url.Values, reqID string) (any, error) {
	names := h.Backend.DescribeVpcEndpointServices()
	resp := &describeVpcEndpointServicesResponse{RequestID: reqID}

	for _, n := range names {
		resp.ServiceNames.Items = append(resp.ServiceNames.Items, serviceNameItem{ServiceName: n})
	}

	return resp, nil
}

func (h *Handler) handleExportKeyPair(vals url.Values, reqID string) (any, error) {
	name := vals.Get("KeyName")

	pubKey, err := h.Backend.ExportKeyPair(name)
	if err != nil {
		return nil, err
	}

	return &exportKeyPairResponse{
		RequestID:         reqID,
		KeyName:           name,
		PublicKeyMaterial: pubKey,
	}, nil
}

func (h *Handler) handleDescribeInstanceTypeOfferings(_ url.Values, reqID string) (any, error) {
	offerings := h.Backend.DescribeInstanceTypeOfferings()
	resp := &describeInstanceTypeOfferingsResponse{RequestID: reqID}

	for _, o := range offerings {
		resp.InstanceTypeOfferingSet.Items = append(
			resp.InstanceTypeOfferingSet.Items,
			instanceTypeOfferingItem{
				InstanceType: o.InstanceType,
				Location:     o.Location,
				LocationType: o.LocationType,
			},
		)
	}

	return resp, nil
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

func (h *Handler) handleDescribeTransitGateways(vals url.Values, reqID string) (any, error) {
	var ids []string

	for i := 1; ; i++ {
		id := vals.Get("TransitGatewayIds.TransitGatewayId." + strconv.Itoa(i))
		if id == "" {
			break
		}

		ids = append(ids, id)
	}

	tgws := h.Backend.DescribeTransitGateways(ids)
	resp := &describeTransitGatewaysResponse{RequestID: reqID}

	for _, tgw := range tgws {
		resp.TransitGatewaySet.Items = append(resp.TransitGatewaySet.Items, transitGatewayItem{
			TransitGatewayID: tgw.ID,
			Description:      tgw.Description,
			State:            tgw.State,
			OwnerID:          tgw.OwnerID,
		})
	}

	return resp, nil
}

func (h *Handler) handleCreateTransitGateway(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")

	tgw, err := h.Backend.CreateTransitGateway(description)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayResponse{
		RequestID: reqID,
		TransitGateway: transitGatewayItem{
			TransitGatewayID: tgw.ID,
			Description:      tgw.Description,
			State:            tgw.State,
			OwnerID:          tgw.OwnerID,
		},
	}, nil
}

func (h *Handler) handleDeleteTransitGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayId")

	if err := h.Backend.DeleteTransitGateway(id); err != nil {
		return nil, err
	}

	return &deleteTransitGatewayResponse{RequestID: reqID, Return: true}, nil
}
