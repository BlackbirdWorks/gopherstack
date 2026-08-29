package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"strings"
)

// ---- Handler registration ----

func registerEC2CoreOps(h *Handler, ops map[string]ec2ActionFn) {
	// Egress-only Internet Gateway
	ops["CreateEgressOnlyInternetGateway"] = h.handleCreateEgressOnlyInternetGateway
	ops["DescribeEgressOnlyInternetGateways"] = h.handleDescribeEgressOnlyInternetGateways
	ops["DeleteEgressOnlyInternetGateway"] = h.handleDeleteEgressOnlyInternetGateway

	// IAM Instance Profile Associations
	ops["AssociateIamInstanceProfile"] = h.handleAssociateIamInstanceProfile
	ops["DisassociateIamInstanceProfile"] = h.handleDisassociateIamInstanceProfile
	ops["DescribeIamInstanceProfileAssociations"] = h.handleDescribeIamInstanceProfileAssociations
	ops["ReplaceIamInstanceProfileAssociation"] = h.handleReplaceIamInstanceProfileAssociation

	// Route table extras
	ops["ReplaceRouteTableAssociation"] = h.handleReplaceRouteTableAssociation

	// VPC CIDR
	ops["AssociateVpcCidrBlock"] = h.handleAssociateVpcCidrBlock

	// Transit Gateway Route Tables
	ops["CreateTransitGatewayRouteTable"] = h.handleCreateTransitGatewayRouteTable
	ops["DescribeTransitGatewayRouteTables"] = h.handleDescribeTransitGatewayRouteTables
	ops["DeleteTransitGatewayRouteTable"] = h.handleDeleteTransitGatewayRouteTable

	// Transit Gateway Routes
	ops["CreateTransitGatewayRoute"] = h.handleCreateTransitGatewayRoute
	ops["DeleteTransitGatewayRoute"] = h.handleDeleteTransitGatewayRoute
	ops["ReplaceTransitGatewayRoute"] = h.handleReplaceTransitGatewayRoute

	// Transit Gateway Route Table Associations
	ops["AssociateTransitGatewayRouteTable"] = h.handleAssociateTransitGatewayRouteTable
	ops["DisassociateTransitGatewayRouteTable"] = h.handleDisassociateTransitGatewayRouteTable
}

func ec2CoreSupportedOperations() []string {
	return []string{
		"CreateEgressOnlyInternetGateway",
		"DescribeEgressOnlyInternetGateways",
		"DeleteEgressOnlyInternetGateway",
		"AssociateIamInstanceProfile",
		"DisassociateIamInstanceProfile",
		"DescribeIamInstanceProfileAssociations",
		"ReplaceIamInstanceProfileAssociation",
		"ReplaceRouteTableAssociation",
		"AssociateVpcCidrBlock",
		"CreateTransitGatewayRouteTable",
		"DescribeTransitGatewayRouteTables",
		"DeleteTransitGatewayRouteTable",
		"CreateTransitGatewayRoute",
		"DeleteTransitGatewayRoute",
		"ReplaceTransitGatewayRoute",
		"AssociateTransitGatewayRouteTable",
		"DisassociateTransitGatewayRouteTable",
	}
}

// ---- XML response types ----

type egressOnlyIGWAttachment struct {
	State string `xml:"state"`
	VpcID string `xml:"vpcId"`
}

type egressOnlyIGWItem struct {
	EgressOnlyInternetGatewayID string                    `xml:"egressOnlyInternetGatewayId"`
	Attachments                 []egressOnlyIGWAttachment `xml:"attachmentSet>item"`
	TagSet                      []simpleTagItem           `xml:"tagSet>item"`
}

type createEgressOnlyInternetGatewayResponse struct {
	XMLName                   xml.Name          `xml:"CreateEgressOnlyInternetGatewayResponse"`
	RequestID                 string            `xml:"requestId"`
	EgressOnlyInternetGateway egressOnlyIGWItem `xml:"egressOnlyInternetGateway"`
}

type describeEgressOnlyInternetGatewaysResponse struct {
	XMLName                    xml.Name `xml:"DescribeEgressOnlyInternetGatewaysResponse"`
	RequestID                  string   `xml:"requestId"`
	EgressOnlyInternetGateways struct {
		Items []egressOnlyIGWItem `xml:"item"`
	} `xml:"egressOnlyInternetGatewaySet"`
}

type deleteEgressOnlyInternetGatewayResponse struct {
	XMLName    xml.Name `xml:"DeleteEgressOnlyInternetGatewayResponse"`
	RequestID  string   `xml:"requestId"`
	ReturnCode bool     `xml:"returnCode"`
}

// iamProfileSpec matches types.IamInstanceProfile (ec2@v1.319.1
// deserializers.go:105766): the second member is "id", not "name" -- this
// backend has no real IAM instance-profile ID, so it approximates with the
// ARN's trailing segment, same as before this key fix.
type iamProfileSpec struct {
	ARN string `xml:"arn"`
	ID  string `xml:"id"`
}

type iamAssociationItem struct {
	AssociationID      string         `xml:"associationId"`
	InstanceID         string         `xml:"instanceId"`
	IamInstanceProfile iamProfileSpec `xml:"iamInstanceProfile"`
	State              string         `xml:"state"`
	Timestamp          string         `xml:"timestamp"`
}

type associateIamInstanceProfileResponse struct {
	XMLName     xml.Name           `xml:"AssociateIamInstanceProfileResponse"`
	RequestID   string             `xml:"requestId"`
	Association iamAssociationItem `xml:"iamInstanceProfileAssociation"`
}

type disassociateIamInstanceProfileResponse struct {
	XMLName     xml.Name           `xml:"DisassociateIamInstanceProfileResponse"`
	RequestID   string             `xml:"requestId"`
	Association iamAssociationItem `xml:"iamInstanceProfileAssociation"`
}

type describeIamInstanceProfileAssociationsResponse struct {
	XMLName      xml.Name `xml:"DescribeIamInstanceProfileAssociationsResponse"`
	RequestID    string   `xml:"requestId"`
	Associations struct {
		Items []iamAssociationItem `xml:"item"`
	} `xml:"iamInstanceProfileAssociationSet"`
}

type replaceIamInstanceProfileAssociationResponse struct {
	XMLName     xml.Name           `xml:"ReplaceIamInstanceProfileAssociationResponse"`
	RequestID   string             `xml:"requestId"`
	Association iamAssociationItem `xml:"iamInstanceProfileAssociation"`
}

type replaceRouteTableAssociationResponse struct {
	XMLName    xml.Name `xml:"ReplaceRouteTableAssociationResponse"`
	RequestID  string   `xml:"requestId"`
	NewAssocID string   `xml:"newAssociationId"`
}

type associateVpcCidrBlockResponse struct {
	XMLName   xml.Name `xml:"AssociateVpcCidrBlockResponse"`
	RequestID string   `xml:"requestId"`
	VpcID     string   `xml:"vpcId"`
	AssocID   string   `xml:"ipv4CidrBlockAssociation>associationId"`
	CidrBlock string   `xml:"ipv4CidrBlockAssociation>cidrBlock"`
	State     string   `xml:"ipv4CidrBlockAssociation>cidrBlockState>state"`
}

type tgwRouteTableItem struct {
	TransitGatewayRouteTableID   string          `xml:"transitGatewayRouteTableId"`
	TransitGatewayID             string          `xml:"transitGatewayId"`
	State                        string          `xml:"state"`
	CreationTime                 string          `xml:"creationTime"`
	TagSet                       []simpleTagItem `xml:"tagSet>item"`
	DefaultAssociationRouteTable bool            `xml:"defaultAssociationRouteTable"`
	DefaultPropagationRouteTable bool            `xml:"defaultPropagationRouteTable"`
}

type createTransitGatewayRouteTableResponse struct {
	XMLName                  xml.Name          `xml:"CreateTransitGatewayRouteTableResponse"`
	RequestID                string            `xml:"requestId"`
	TransitGatewayRouteTable tgwRouteTableItem `xml:"transitGatewayRouteTable"`
}

type describeTransitGatewayRouteTablesResponse struct {
	XMLName                   xml.Name `xml:"DescribeTransitGatewayRouteTablesResponse"`
	RequestID                 string   `xml:"requestId"`
	TransitGatewayRouteTables struct {
		Items []tgwRouteTableItem `xml:"item"`
	} `xml:"transitGatewayRouteTables"`
}

type deleteTransitGatewayRouteTableResponse struct {
	XMLName                  xml.Name          `xml:"DeleteTransitGatewayRouteTableResponse"`
	RequestID                string            `xml:"requestId"`
	TransitGatewayRouteTable tgwRouteTableItem `xml:"transitGatewayRouteTable"`
}

type tgwRouteAttachmentItem struct {
	TransitGatewayAttachmentID string `xml:"transitGatewayAttachmentId"`
	ResourceID                 string `xml:"resourceId,omitempty"`
	ResourceType               string `xml:"resourceType"`
}

type tgwRouteItem struct {
	DestinationCidrBlock      string                   `xml:"destinationCidrBlock"`
	State                     string                   `xml:"state"`
	Type                      string                   `xml:"type"`
	TransitGatewayAttachments []tgwRouteAttachmentItem `xml:"transitGatewayAttachments>item"`
}

type createTransitGatewayRouteResponse struct {
	XMLName   xml.Name     `xml:"CreateTransitGatewayRouteResponse"`
	RequestID string       `xml:"requestId"`
	Route     tgwRouteItem `xml:"route"`
}

type deleteTransitGatewayRouteResponse struct {
	XMLName   xml.Name     `xml:"DeleteTransitGatewayRouteResponse"`
	RequestID string       `xml:"requestId"`
	Route     tgwRouteItem `xml:"route"`
}

type replaceTransitGatewayRouteResponse struct {
	XMLName   xml.Name     `xml:"ReplaceTransitGatewayRouteResponse"`
	RequestID string       `xml:"requestId"`
	Route     tgwRouteItem `xml:"route"`
}

type tgwRTAssociationItem struct {
	TransitGatewayRouteTableID string `xml:"transitGatewayRouteTableId"`
	TransitGatewayAttachmentID string `xml:"transitGatewayAttachmentId"`
	ResourceType               string `xml:"resourceType"`
	State                      string `xml:"state"`
}

type associateTransitGatewayRouteTableResponse struct {
	XMLName     xml.Name             `xml:"AssociateTransitGatewayRouteTableResponse"`
	RequestID   string               `xml:"requestId"`
	Association tgwRTAssociationItem `xml:"association"`
}

type disassociateTransitGatewayRouteTableResponse struct {
	XMLName     xml.Name             `xml:"DisassociateTransitGatewayRouteTableResponse"`
	RequestID   string               `xml:"requestId"`
	Association tgwRTAssociationItem `xml:"association"`
}

// ---- Handlers ----

func (h *Handler) handleCreateEgressOnlyInternetGateway(
	vals url.Values,
	reqID string,
) (any, error) {
	vpcID := vals.Get("VpcId")
	igw, err := h.Backend.CreateEgressOnlyInternetGateway(vpcID)
	if err != nil {
		return nil, err
	}

	return &createEgressOnlyInternetGatewayResponse{
		RequestID:                 reqID,
		EgressOnlyInternetGateway: toEgressOnlyIGWItem(igw, nil),
	}, nil
}

func toEgressOnlyIGWItem(igw *EgressOnlyInternetGateway, tags map[string]string) egressOnlyIGWItem {
	return egressOnlyIGWItem{
		EgressOnlyInternetGatewayID: igw.ID,
		Attachments: []egressOnlyIGWAttachment{
			{State: igw.State, VpcID: igw.VPCID},
		},
		TagSet: tagItemsFromMap(tags),
	}
}

func (h *Handler) handleDescribeEgressOnlyInternetGateways(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "EgressOnlyInternetGatewayId")
	igws := h.Backend.DescribeEgressOnlyInternetGateways(ids)

	resp := &describeEgressOnlyInternetGatewaysResponse{RequestID: reqID}

	for _, igw := range igws {
		resp.EgressOnlyInternetGateways.Items = append(
			resp.EgressOnlyInternetGateways.Items,
			toEgressOnlyIGWItem(igw, h.Backend.TagsForResource(igw.ID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteEgressOnlyInternetGateway(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("EgressOnlyInternetGatewayId")

	if err := h.Backend.DeleteEgressOnlyInternetGateway(id); err != nil {
		return nil, err
	}

	return &deleteEgressOnlyInternetGatewayResponse{RequestID: reqID, ReturnCode: true}, nil
}

func iamAssocToItem(assoc *IamInstanceProfileAssociation) iamAssociationItem {
	return iamAssociationItem{
		AssociationID: assoc.AssociationID,
		InstanceID:    assoc.InstanceID,
		IamInstanceProfile: iamProfileSpec{
			ARN: assoc.IamInstanceProfile,
			ID:  iamProfileName(assoc.IamInstanceProfile),
		},
		State:     assoc.State,
		Timestamp: assoc.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// iamProfileName extracts the profile name from an ARN (last segment), or returns the value as-is.
func iamProfileName(arnOrName string) string {
	parts := strings.Split(arnOrName, "/")

	return parts[len(parts)-1]
}

func (h *Handler) handleAssociateIamInstanceProfile(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	profileARN := vals.Get("IamInstanceProfile.Arn")

	if profileARN == "" {
		profileARN = vals.Get("IamInstanceProfile.Name")
	}

	assoc, err := h.Backend.AssociateIamInstanceProfile(instanceID, profileARN)
	if err != nil {
		return nil, err
	}

	return &associateIamInstanceProfileResponse{
		RequestID:   reqID,
		Association: iamAssocToItem(assoc),
	}, nil
}

func (h *Handler) handleDisassociateIamInstanceProfile(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	assoc, err := h.Backend.DisassociateIamInstanceProfile(assocID)
	if err != nil {
		return nil, err
	}

	return &disassociateIamInstanceProfileResponse{
		RequestID:   reqID,
		Association: iamAssocToItem(assoc),
	}, nil
}

func (h *Handler) handleDescribeIamInstanceProfileAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	assocIDs := parseMemberList(vals, "AssociationId")
	instanceID := vals.Get("Filter.1.Value.1") // basic filter support

	// Try direct instance ID filter.
	for i := 1; ; i++ {
		key := "Filter." + strconv.Itoa(i) + ".Name"
		name := vals.Get(key)

		if name == "" {
			break
		}

		if name == filterKeyInstanceID {
			instanceID = vals.Get("Filter." + strconv.Itoa(i) + ".Value.1")
		}
	}

	assocs := h.Backend.DescribeIamInstanceProfileAssociations(assocIDs, instanceID)

	resp := &describeIamInstanceProfileAssociationsResponse{RequestID: reqID}

	for _, a := range assocs {
		resp.Associations.Items = append(resp.Associations.Items, iamAssocToItem(a))
	}

	return resp, nil
}

func (h *Handler) handleReplaceIamInstanceProfileAssociation(
	vals url.Values,
	reqID string,
) (any, error) {
	assocID := vals.Get("AssociationId")
	profileARN := vals.Get("IamInstanceProfile.Arn")

	if profileARN == "" {
		profileARN = vals.Get("IamInstanceProfile.Name")
	}

	assoc, err := h.Backend.ReplaceIamInstanceProfileAssociation(assocID, profileARN)
	if err != nil {
		return nil, err
	}

	return &replaceIamInstanceProfileAssociationResponse{
		RequestID:   reqID,
		Association: iamAssocToItem(assoc),
	}, nil
}

func (h *Handler) handleReplaceRouteTableAssociation(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	rtID := vals.Get("RouteTableId")

	newAssocID, err := h.Backend.ReplaceRouteTableAssociation(assocID, rtID)
	if err != nil {
		return nil, err
	}

	return &replaceRouteTableAssociationResponse{
		RequestID:  reqID,
		NewAssocID: newAssocID,
	}, nil
}

func (h *Handler) handleAssociateVpcCidrBlock(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	cidr := vals.Get("CidrBlock")

	assoc, err := h.Backend.AssociateVpcCidrBlock(vpcID, cidr)
	if err != nil {
		return nil, err
	}

	return &associateVpcCidrBlockResponse{
		RequestID: reqID,
		VpcID:     vpcID,
		AssocID:   assoc.AssociationID,
		CidrBlock: assoc.CidrBlock,
		State:     assoc.State,
	}, nil
}

func tgwRTToItem(rt *TransitGatewayRouteTable, tags map[string]string) tgwRouteTableItem {
	return tgwRouteTableItem{
		TransitGatewayRouteTableID:   rt.RouteTableID,
		TransitGatewayID:             rt.TransitGatewayID,
		State:                        rt.State,
		DefaultAssociationRouteTable: rt.DefaultAssociation,
		DefaultPropagationRouteTable: rt.DefaultPropagation,
		CreationTime:                 rt.CreateTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		TagSet:                       tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateTransitGatewayRouteTable(vals url.Values, reqID string) (any, error) {
	tgwID := vals.Get("TransitGatewayId")
	tags := parseTagSpecificationPlural(vals, "transit-gateway-route-table")

	rt, err := h.Backend.CreateTransitGatewayRouteTable(tgwID, tags)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayRouteTableResponse{
		RequestID:                reqID,
		TransitGatewayRouteTable: tgwRTToItem(rt, tags),
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayRouteTables(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayRouteTableId")
	rts := h.Backend.DescribeTransitGatewayRouteTables(ids)

	resp := &describeTransitGatewayRouteTablesResponse{RequestID: reqID}

	for _, rt := range rts {
		resp.TransitGatewayRouteTables.Items = append(
			resp.TransitGatewayRouteTables.Items,
			tgwRTToItem(rt, h.Backend.TagsForResource(rt.RouteTableID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteTransitGatewayRouteTable(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayRouteTableId")

	// Capture state before deletion for the response.
	existing := h.Backend.DescribeTransitGatewayRouteTables([]string{id})

	if err := h.Backend.DeleteTransitGatewayRouteTable(id); err != nil {
		return nil, err
	}

	var item tgwRouteTableItem

	if len(existing) > 0 {
		item = tgwRTToItem(existing[0], h.Backend.TagsForResource(existing[0].RouteTableID))
		item.State = tgwRouteStateDeleted
	}

	return &deleteTransitGatewayRouteTableResponse{
		RequestID:                reqID,
		TransitGatewayRouteTable: item,
	}, nil
}

func tgwRouteToItem(r *TransitGatewayRoute) tgwRouteItem {
	item := tgwRouteItem{
		DestinationCidrBlock: r.DestinationCidrBlock,
		State:                r.State,
		Type:                 r.Type,
	}

	// A blackhole route has no attachment; every other route's ResourceType
	// must reflect the attachment's real kind (vpc/peering/connect/client-vpn),
	// not be hardcoded - previously this always reported "vpc" regardless of
	// the attachment's real type, and never reported ResourceId at all,
	// matching the same bug class already fixed for
	// Associate/DisassociateTransitGatewayRouteTable.
	if r.TransitGatewayAttachmentID != "" {
		item.TransitGatewayAttachments = []tgwRouteAttachmentItem{
			{
				TransitGatewayAttachmentID: r.TransitGatewayAttachmentID,
				ResourceID:                 r.ResourceID,
				ResourceType:               r.ResourceType,
			},
		}
	}

	return item
}

func (h *Handler) handleCreateTransitGatewayRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("TransitGatewayRouteTableId")
	cidr := vals.Get("DestinationCidrBlock")
	attachID := vals.Get("TransitGatewayAttachmentId")
	blackhole := vals.Get("Blackhole") == ec2BooleanTrue

	route, err := h.Backend.CreateTransitGatewayRoute(rtID, cidr, attachID, blackhole)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayRouteResponse{
		RequestID: reqID,
		Route:     tgwRouteToItem(route),
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("TransitGatewayRouteTableId")
	cidr := vals.Get("DestinationCidrBlock")

	// Capture before deleting for the response.
	route := &TransitGatewayRoute{
		DestinationCidrBlock:       cidr,
		TransitGatewayRouteTableID: rtID,
		State:                      tgwRouteStateDeleted,
		Type:                       tgwRouteTypeStatic,
	}

	if err := h.Backend.DeleteTransitGatewayRoute(rtID, cidr); err != nil {
		return nil, err
	}

	return &deleteTransitGatewayRouteResponse{
		RequestID: reqID,
		Route:     tgwRouteToItem(route),
	}, nil
}

func (h *Handler) handleReplaceTransitGatewayRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("TransitGatewayRouteTableId")
	cidr := vals.Get("DestinationCidrBlock")
	attachID := vals.Get("TransitGatewayAttachmentId")
	blackhole := vals.Get("Blackhole") == ec2BooleanTrue

	route, err := h.Backend.ReplaceTransitGatewayRoute(rtID, cidr, attachID, blackhole)
	if err != nil {
		return nil, err
	}

	return &replaceTransitGatewayRouteResponse{
		RequestID: reqID,
		Route:     tgwRouteToItem(route),
	}, nil
}

func (h *Handler) handleAssociateTransitGatewayRouteTable(
	vals url.Values,
	reqID string,
) (any, error) {
	rtID := vals.Get("TransitGatewayRouteTableId")
	attachID := vals.Get("TransitGatewayAttachmentId")

	assoc, err := h.Backend.AssociateTransitGatewayRouteTable(rtID, attachID)
	if err != nil {
		return nil, err
	}

	return &associateTransitGatewayRouteTableResponse{
		RequestID: reqID,
		Association: tgwRTAssociationItem{
			TransitGatewayRouteTableID: assoc.TransitGatewayRouteTableID,
			TransitGatewayAttachmentID: assoc.TransitGatewayAttachmentID,
			ResourceType:               assoc.ResourceType,
			State:                      assoc.State,
		},
	}, nil
}

func (h *Handler) handleDisassociateTransitGatewayRouteTable(
	vals url.Values,
	reqID string,
) (any, error) {
	rtID := vals.Get("TransitGatewayRouteTableId")
	attachID := vals.Get("TransitGatewayAttachmentId")

	if err := h.Backend.DisassociateTransitGatewayRouteTable(rtID, attachID); err != nil {
		return nil, err
	}

	return &disassociateTransitGatewayRouteTableResponse{
		RequestID: reqID,
		Association: tgwRTAssociationItem{
			TransitGatewayRouteTableID: rtID,
			TransitGatewayAttachmentID: attachID,
			State:                      "disassociated",
		},
	}, nil
}
