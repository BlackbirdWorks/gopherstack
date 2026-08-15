package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// ---- Handler registration ----

// registerTGWPeripheralsOps registers the TGWPeripherals operation handlers.
func registerTGWPeripheralsOps(h *Handler, ops map[string]ec2ActionFn) {
	// Transit Gateway Policy Tables
	ops["CreateTransitGatewayPolicyTable"] = h.handleCreateTransitGatewayPolicyTable
	ops["DescribeTransitGatewayPolicyTables"] = h.handleDescribeTransitGatewayPolicyTables
	ops["DeleteTransitGatewayPolicyTable"] = h.handleDeleteTransitGatewayPolicyTable
	ops["AssociateTransitGatewayPolicyTable"] = h.handleAssociateTransitGatewayPolicyTable
	ops["DisassociateTransitGatewayPolicyTable"] = h.handleDisassociateTransitGatewayPolicyTable
	ops["GetTransitGatewayPolicyTableAssociations"] = h.handleGetTransitGatewayPolicyTableAssociations
	ops["GetTransitGatewayPolicyTableEntries"] = h.handleGetTransitGatewayPolicyTableEntries
	ops["CreateTransitGatewayPolicyTableEntry"] = h.handleCreateTransitGatewayPolicyTableEntry
	ops["ModifyTransitGatewayPolicyTableEntry"] = h.handleModifyTransitGatewayPolicyTableEntry
	ops["DeleteTransitGatewayPolicyTableEntry"] = h.handleDeleteTransitGatewayPolicyTableEntry

	// Transit Gateway Route Table Announcements
	ops["CreateTransitGatewayRouteTableAnnouncement"] = h.handleCreateTransitGatewayRouteTableAnnouncement
	ops["DeleteTransitGatewayRouteTableAnnouncement"] = h.handleDeleteTransitGatewayRouteTableAnnouncement
	ops["DescribeTransitGatewayRouteTableAnnouncements"] = h.handleDescribeTransitGatewayRouteTableAnnouncements

	// Transit Gateway Route Table associations / propagations / search / export
	ops["GetTransitGatewayRouteTableAssociations"] = h.handleGetTransitGatewayRouteTableAssociations
	ops["GetTransitGatewayRouteTablePropagations"] = h.handleGetTransitGatewayRouteTablePropagations
	ops["GetTransitGatewayAttachmentPropagations"] = h.handleGetTransitGatewayAttachmentPropagations
	ops["SearchTransitGatewayRoutes"] = h.handleSearchTransitGatewayRoutes
	ops["ExportTransitGatewayRoutes"] = h.handleExportTransitGatewayRoutes

	// Transit Gateway modifications
	ops["ModifyTransitGatewayVpcAttachment"] = h.handleModifyTransitGatewayVpcAttachment
	ops["ModifyTransitGatewayMeteringPolicy"] = h.handleModifyTransitGatewayMeteringPolicy
	ops["GetTransitGatewayMeteringPolicyEntries"] = h.handleGetTransitGatewayMeteringPolicyEntries
	ops["ModifyTransitGatewayPrefixListReference"] = h.handleModifyTransitGatewayPrefixListReference

	// Transit Gateway rejections
	ops["RejectTransitGatewayVpcAttachment"] = h.handleRejectTransitGatewayVpcAttachment
	ops["RejectTransitGatewayPeeringAttachment"] = h.handleRejectTransitGatewayPeeringAttachment
	ops["RejectTransitGatewayMulticastDomainAssociations"] = h.handleRejectTransitGatewayMulticastDomainAssociations
}

func tgwPeripheralsSupportedOperations() []string {
	return []string{
		"CreateTransitGatewayPolicyTable",
		"DescribeTransitGatewayPolicyTables",
		"DeleteTransitGatewayPolicyTable",
		"AssociateTransitGatewayPolicyTable",
		"DisassociateTransitGatewayPolicyTable",
		"GetTransitGatewayPolicyTableAssociations",
		"GetTransitGatewayPolicyTableEntries",
		"CreateTransitGatewayPolicyTableEntry",
		"ModifyTransitGatewayPolicyTableEntry",
		"DeleteTransitGatewayPolicyTableEntry",
		"CreateTransitGatewayRouteTableAnnouncement",
		"DeleteTransitGatewayRouteTableAnnouncement",
		"DescribeTransitGatewayRouteTableAnnouncements",
		"GetTransitGatewayRouteTableAssociations",
		"GetTransitGatewayRouteTablePropagations",
		"GetTransitGatewayAttachmentPropagations",
		"SearchTransitGatewayRoutes",
		"ExportTransitGatewayRoutes",
		"ModifyTransitGatewayVpcAttachment",
		"ModifyTransitGatewayMeteringPolicy",
		"GetTransitGatewayMeteringPolicyEntries",
		"ModifyTransitGatewayPrefixListReference",
		"RejectTransitGatewayVpcAttachment",
		"RejectTransitGatewayPeeringAttachment",
		"RejectTransitGatewayMulticastDomainAssociations",
	}
}

// ---- XML types: policy tables ----

type tgwPolicyTableItem struct {
	CreationTime                string `xml:"creationTime,omitempty"`
	TransitGatewayID            string `xml:"transitGatewayId,omitempty"`
	TransitGatewayPolicyTableID string `xml:"transitGatewayPolicyTableId,omitempty"`
	State                       string `xml:"state,omitempty"`
}

func tgwPolicyTableToItem(pt *TransitGatewayPolicyTable) tgwPolicyTableItem {
	return tgwPolicyTableItem{
		CreationTime:                pt.CreationTime.UTC().Format(timeLayoutISO),
		TransitGatewayID:            pt.TransitGatewayID,
		TransitGatewayPolicyTableID: pt.TransitGatewayPolicyTableID,
		State:                       pt.State,
	}
}

type createTransitGatewayPolicyTableResponse struct {
	XMLName     xml.Name           `xml:"CreateTransitGatewayPolicyTableResponse"`
	Xmlns       string             `xml:"xmlns,attr"`
	RequestID   string             `xml:"requestId"`
	PolicyTable tgwPolicyTableItem `xml:"transitGatewayPolicyTable"`
}

type describeTransitGatewayPolicyTablesResponse struct {
	XMLName      xml.Name `xml:"DescribeTransitGatewayPolicyTablesResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	PolicyTables struct {
		Items []tgwPolicyTableItem `xml:"item"`
	} `xml:"transitGatewayPolicyTables"`
}

type deleteTransitGatewayPolicyTableResponse struct {
	XMLName     xml.Name           `xml:"DeleteTransitGatewayPolicyTableResponse"`
	Xmlns       string             `xml:"xmlns,attr"`
	RequestID   string             `xml:"requestId"`
	PolicyTable tgwPolicyTableItem `xml:"transitGatewayPolicyTable"`
}

type tgwPolicyTableAssociationItem struct {
	TransitGatewayPolicyTableID string `xml:"transitGatewayPolicyTableId,omitempty"`
	TransitGatewayAttachmentID  string `xml:"transitGatewayAttachmentId,omitempty"`
	ResourceID                  string `xml:"resourceId,omitempty"`
	ResourceType                string `xml:"resourceType,omitempty"`
	State                       string `xml:"state,omitempty"`
}

func tgwPolicyTableAssociationToItem(
	a *TransitGatewayPolicyTableAssociation,
) tgwPolicyTableAssociationItem {
	return tgwPolicyTableAssociationItem{
		TransitGatewayPolicyTableID: a.TransitGatewayPolicyTableID,
		TransitGatewayAttachmentID:  a.TransitGatewayAttachmentID,
		ResourceID:                  a.ResourceID,
		ResourceType:                a.ResourceType,
		State:                       a.State,
	}
}

type associateTransitGatewayPolicyTableResponse struct {
	XMLName     xml.Name                      `xml:"AssociateTransitGatewayPolicyTableResponse"`
	Xmlns       string                        `xml:"xmlns,attr"`
	RequestID   string                        `xml:"requestId"`
	Association tgwPolicyTableAssociationItem `xml:"association"`
}

type disassociateTransitGatewayPolicyTableResponse struct {
	XMLName     xml.Name                      `xml:"DisassociateTransitGatewayPolicyTableResponse"`
	Xmlns       string                        `xml:"xmlns,attr"`
	RequestID   string                        `xml:"requestId"`
	Association tgwPolicyTableAssociationItem `xml:"association"`
}

type getTransitGatewayPolicyTableAssociationsResponse struct {
	XMLName      xml.Name `xml:"GetTransitGatewayPolicyTableAssociationsResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	Associations struct {
		Items []tgwPolicyTableAssociationItem `xml:"item"`
	} `xml:"associations"`
}

// tgwPolicyRuleItem mirrors the real AWS TransitGatewayPolicyRule shape
// (field-diffed against the installed SDK's
// awsEc2query_deserializeDocumentTransitGatewayPolicyRule).
type tgwPolicyRuleItem struct {
	MetaData             *tgwPolicyRuleMetaDataItem `xml:"metaData,omitempty"`
	SourceCidrBlock      string                     `xml:"sourceCidrBlock,omitempty"`
	SourcePortRange      string                     `xml:"sourcePortRange,omitempty"`
	DestinationCidrBlock string                     `xml:"destinationCidrBlock,omitempty"`
	DestinationPortRange string                     `xml:"destinationPortRange,omitempty"`
	Protocol             string                     `xml:"protocol,omitempty"`
}

// tgwPolicyRuleMetaDataItem mirrors the real AWS TransitGatewayPolicyRuleMetaData shape.
type tgwPolicyRuleMetaDataItem struct {
	MetaDataKey   string `xml:"metaDataKey,omitempty"`
	MetaDataValue string `xml:"metaDataValue,omitempty"`
}

// tgwPolicyTableEntryItem mirrors the real AWS TransitGatewayPolicyTableEntry
// shape (field-diffed against the installed SDK's
// awsEc2query_deserializeDocumentTransitGatewayPolicyTableEntry).
type tgwPolicyTableEntryItem struct {
	PolicyRule         tgwPolicyRuleItem `xml:"policyRule"`
	TargetRouteTableID string            `xml:"targetRouteTableId,omitempty"`
	State              string            `xml:"state,omitempty"`
	PolicyRuleNumber   int               `xml:"policyRuleNumber,omitempty"`
}

func tgwPolicyTableEntryToItem(e *TransitGatewayPolicyTableEntry) tgwPolicyTableEntryItem {
	item := tgwPolicyTableEntryItem{
		PolicyRuleNumber:   e.PolicyRuleNumber,
		TargetRouteTableID: e.TargetRouteTableID,
		State:              e.State,
		PolicyRule: tgwPolicyRuleItem{
			SourceCidrBlock:      e.SourceCidrBlock,
			SourcePortRange:      e.SourcePortRange,
			DestinationCidrBlock: e.DestinationCidrBlock,
			DestinationPortRange: e.DestinationPortRange,
			Protocol:             e.Protocol,
		},
	}

	if e.MetaDataKey != "" || e.MetaDataValue != "" {
		item.PolicyRule.MetaData = &tgwPolicyRuleMetaDataItem{
			MetaDataKey:   e.MetaDataKey,
			MetaDataValue: e.MetaDataValue,
		}
	}

	return item
}

type getTransitGatewayPolicyTableEntriesResponse struct {
	XMLName   xml.Name `xml:"GetTransitGatewayPolicyTableEntriesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Entries   struct {
		Items []tgwPolicyTableEntryItem `xml:"item"`
	} `xml:"transitGatewayPolicyTableEntries"`
}

type createTransitGatewayPolicyTableEntryResponse struct {
	XMLName   xml.Name                `xml:"CreateTransitGatewayPolicyTableEntryResponse"`
	Xmlns     string                  `xml:"xmlns,attr"`
	RequestID string                  `xml:"requestId"`
	Entry     tgwPolicyTableEntryItem `xml:"transitGatewayPolicyTableEntry"`
}

type modifyTransitGatewayPolicyTableEntryResponse struct {
	XMLName   xml.Name                `xml:"ModifyTransitGatewayPolicyTableEntryResponse"`
	Xmlns     string                  `xml:"xmlns,attr"`
	RequestID string                  `xml:"requestId"`
	Entry     tgwPolicyTableEntryItem `xml:"transitGatewayPolicyTableEntry"`
}

type deleteTransitGatewayPolicyTableEntryResponse struct {
	XMLName   xml.Name                `xml:"DeleteTransitGatewayPolicyTableEntryResponse"`
	Xmlns     string                  `xml:"xmlns,attr"`
	RequestID string                  `xml:"requestId"`
	Entry     tgwPolicyTableEntryItem `xml:"transitGatewayPolicyTableEntry"`
}

// ---- XML types: route table announcements ----

type tgwRouteTableAnnouncementItem struct {
	CreationTime                           string          `xml:"creationTime,omitempty"`
	PeeringAttachmentID                    string          `xml:"peeringAttachmentId,omitempty"`
	AnnouncementDirection                  string          `xml:"announcementDirection,omitempty"`
	State                                  string          `xml:"state,omitempty"`
	TransitGatewayID                       string          `xml:"transitGatewayId,omitempty"`
	TransitGatewayRouteTableAnnouncementID string          `xml:"transitGatewayRouteTableAnnouncementId,omitempty"`
	TransitGatewayRouteTableID             string          `xml:"transitGatewayRouteTableId,omitempty"`
	PeerTransitGatewayID                   string          `xml:"peerTransitGatewayId,omitempty"`
	TagSet                                 []simpleTagItem `xml:"tagSet>item"`
}

func tgwRouteTableAnnouncementToItem(
	a *TransitGatewayRouteTableAnnouncement,
	tags map[string]string,
) tgwRouteTableAnnouncementItem {
	return tgwRouteTableAnnouncementItem{
		CreationTime:                           a.CreationTime.UTC().Format(timeLayoutISO),
		PeeringAttachmentID:                    a.PeeringAttachmentID,
		AnnouncementDirection:                  a.AnnouncementDirection,
		State:                                  a.State,
		TransitGatewayID:                       a.TransitGatewayID,
		TransitGatewayRouteTableAnnouncementID: a.TransitGatewayRouteTableAnnouncementID,
		TransitGatewayRouteTableID:             a.TransitGatewayRouteTableID,
		PeerTransitGatewayID:                   a.PeerTransitGatewayID,
		TagSet:                                 tagItemsFromMap(tags),
	}
}

type createTransitGatewayRouteTableAnnouncementResponse struct {
	XMLName      xml.Name                      `xml:"CreateTransitGatewayRouteTableAnnouncementResponse"`
	Xmlns        string                        `xml:"xmlns,attr"`
	RequestID    string                        `xml:"requestId"`
	Announcement tgwRouteTableAnnouncementItem `xml:"transitGatewayRouteTableAnnouncement"`
}

type deleteTransitGatewayRouteTableAnnouncementResponse struct {
	XMLName      xml.Name                      `xml:"DeleteTransitGatewayRouteTableAnnouncementResponse"`
	Xmlns        string                        `xml:"xmlns,attr"`
	RequestID    string                        `xml:"requestId"`
	Announcement tgwRouteTableAnnouncementItem `xml:"transitGatewayRouteTableAnnouncement"`
}

type describeTransitGatewayRouteTableAnnouncementsResponse struct {
	XMLName       xml.Name `xml:"DescribeTransitGatewayRouteTableAnnouncementsResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	Announcements struct {
		Items []tgwRouteTableAnnouncementItem `xml:"item"`
	} `xml:"transitGatewayRouteTableAnnouncements"`
}

// ---- XML types: route table associations / propagations / search / export ----

type tgwRTAssociationGetItem struct {
	ResourceID                 string `xml:"resourceId,omitempty"`
	ResourceType               string `xml:"resourceType,omitempty"`
	State                      string `xml:"state,omitempty"`
	TransitGatewayAttachmentID string `xml:"transitGatewayAttachmentId,omitempty"`
}

type getTransitGatewayRouteTableAssociationsResponse struct {
	XMLName      xml.Name `xml:"GetTransitGatewayRouteTableAssociationsResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	Associations struct {
		Items []tgwRTAssociationGetItem `xml:"item"`
	} `xml:"associations"`
}

type tgwRTPropagationItem struct {
	ResourceID                             string `xml:"resourceId,omitempty"`
	ResourceType                           string `xml:"resourceType,omitempty"`
	State                                  string `xml:"state,omitempty"`
	TransitGatewayAttachmentID             string `xml:"transitGatewayAttachmentId,omitempty"`
	TransitGatewayRouteTableAnnouncementID string `xml:"transitGatewayRouteTableAnnouncementId,omitempty"`
}

type getTransitGatewayRouteTablePropagationsResponse struct {
	XMLName      xml.Name `xml:"GetTransitGatewayRouteTablePropagationsResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	Propagations struct {
		Items []tgwRTPropagationItem `xml:"item"`
	} `xml:"transitGatewayRouteTablePropagations"`
}

type tgwAttachmentPropagationItem struct {
	TransitGatewayRouteTableID string `xml:"transitGatewayRouteTableId,omitempty"`
	State                      string `xml:"state,omitempty"`
}

type getTransitGatewayAttachmentPropagationsResponse struct {
	XMLName      xml.Name `xml:"GetTransitGatewayAttachmentPropagationsResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	Propagations struct {
		Items []tgwAttachmentPropagationItem `xml:"item"`
	} `xml:"transitGatewayAttachmentPropagations"`
}

type searchTransitGatewayRoutesResponse struct {
	XMLName   xml.Name `xml:"SearchTransitGatewayRoutesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	RouteSet  struct {
		Items []tgwRouteItem `xml:"item"`
	} `xml:"routeSet"`
	AdditionalRoutesAvailable bool `xml:"additionalRoutesAvailable"`
}

type exportTransitGatewayRoutesResponse struct {
	XMLName    xml.Name `xml:"ExportTransitGatewayRoutesResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	S3Location string   `xml:"s3Location"`
}

// ---- XML types: modifications / rejections ----

type modifyTransitGatewayVpcAttachmentResponse struct {
	XMLName    xml.Name             `xml:"ModifyTransitGatewayVpcAttachmentResponse"`
	Xmlns      string               `xml:"xmlns,attr"`
	RequestID  string               `xml:"requestId"`
	Attachment tgwVpcAttachmentItem `xml:"transitGatewayVpcAttachment"`
}

type modifyTransitGatewayMeteringPolicyResponse struct {
	XMLName   xml.Name              `xml:"ModifyTransitGatewayMeteringPolicyResponse"`
	Xmlns     string                `xml:"xmlns,attr"`
	RequestID string                `xml:"requestId"`
	Policy    tgwMeteringPolicyItem `xml:"transitGatewayMeteringPolicy"`
}

type getTransitGatewayMeteringPolicyEntriesResponse struct {
	XMLName   xml.Name `xml:"GetTransitGatewayMeteringPolicyEntriesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Entries   struct {
		Items []tgwMeteringPolicyEntryItem `xml:"item"`
	} `xml:"transitGatewayMeteringPolicyEntries"`
}

type modifyTransitGatewayPrefixListReferenceResponse struct {
	XMLName   xml.Name             `xml:"ModifyTransitGatewayPrefixListReferenceResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	RequestID string               `xml:"requestId"`
	Reference tgwPrefixListRefItem `xml:"transitGatewayPrefixListReference"`
}

type rejectTransitGatewayVpcAttachmentResponse struct {
	XMLName    xml.Name             `xml:"RejectTransitGatewayVpcAttachmentResponse"`
	Xmlns      string               `xml:"xmlns,attr"`
	RequestID  string               `xml:"requestId"`
	Attachment tgwVpcAttachmentItem `xml:"transitGatewayVpcAttachment"`
}

type rejectTransitGatewayPeeringAttachmentResponse struct {
	XMLName    xml.Name                 `xml:"RejectTransitGatewayPeeringAttachmentResponse"`
	Xmlns      string                   `xml:"xmlns,attr"`
	RequestID  string                   `xml:"requestId"`
	Attachment tgwPeeringAttachmentItem `xml:"transitGatewayPeeringAttachment"`
}

type rejectTransitGatewayMulticastDomainAssociationsResponse struct {
	XMLName      xml.Name                         `xml:"RejectTransitGatewayMulticastDomainAssociationsResponse"`
	Xmlns        string                           `xml:"xmlns,attr"`
	RequestID    string                           `xml:"requestId"`
	Associations tgwMulticastDomainAssociationSet `xml:"associations"`
}

// ---- Handlers: policy tables ----

func (h *Handler) handleCreateTransitGatewayPolicyTable(vals url.Values, reqID string) (any, error) {
	pt, err := h.Backend.CreateTransitGatewayPolicyTable(vals.Get("TransitGatewayId"))
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayPolicyTableResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		PolicyTable: tgwPolicyTableToItem(pt),
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayPolicyTables(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayPolicyTableIds")
	pts := h.Backend.DescribeTransitGatewayPolicyTables(ids)

	resp := &describeTransitGatewayPolicyTablesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, pt := range pts {
		resp.PolicyTables.Items = append(resp.PolicyTables.Items, tgwPolicyTableToItem(pt))
	}

	return resp, nil
}

func (h *Handler) handleDeleteTransitGatewayPolicyTable(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayPolicyTableId")

	existing := h.Backend.DescribeTransitGatewayPolicyTables([]string{id})
	if err := h.Backend.DeleteTransitGatewayPolicyTable(id); err != nil {
		return nil, err
	}

	var item tgwPolicyTableItem
	if len(existing) > 0 {
		item = tgwPolicyTableToItem(existing[0])
		item.State = tgwRouteStateDeleted
	}

	return &deleteTransitGatewayPolicyTableResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		PolicyTable: item,
	}, nil
}

func (h *Handler) handleAssociateTransitGatewayPolicyTable(
	vals url.Values,
	reqID string,
) (any, error) {
	policyTableID := vals.Get("TransitGatewayPolicyTableId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	assoc, err := h.Backend.AssociateTransitGatewayPolicyTable(policyTableID, attachmentID)
	if err != nil {
		return nil, err
	}

	return &associateTransitGatewayPolicyTableResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		Association: tgwPolicyTableAssociationToItem(assoc),
	}, nil
}

func (h *Handler) handleDisassociateTransitGatewayPolicyTable(
	vals url.Values,
	reqID string,
) (any, error) {
	policyTableID := vals.Get("TransitGatewayPolicyTableId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	assoc, err := h.Backend.DisassociateTransitGatewayPolicyTable(policyTableID, attachmentID)
	if err != nil {
		return nil, err
	}

	return &disassociateTransitGatewayPolicyTableResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		Association: tgwPolicyTableAssociationToItem(assoc),
	}, nil
}

func (h *Handler) handleGetTransitGatewayPolicyTableAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	policyTableID := vals.Get("TransitGatewayPolicyTableId")
	assocs := h.Backend.GetTransitGatewayPolicyTableAssociations(policyTableID)

	resp := &getTransitGatewayPolicyTableAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.Associations.Items = append(resp.Associations.Items, tgwPolicyTableAssociationToItem(a))
	}

	return resp, nil
}

func (h *Handler) handleGetTransitGatewayPolicyTableEntries(
	vals url.Values,
	reqID string,
) (any, error) {
	policyTableID := vals.Get("TransitGatewayPolicyTableId")

	entries, err := h.Backend.GetTransitGatewayPolicyTableEntries(policyTableID)
	if err != nil {
		return nil, err
	}

	resp := &getTransitGatewayPolicyTableEntriesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, e := range entries {
		resp.Entries.Items = append(resp.Entries.Items, tgwPolicyTableEntryToItem(e))
	}

	return resp, nil
}

// policyRuleFromVals builds the PolicyRule-matching fields of a
// TransitGatewayPolicyTableEntry from the wire's "PolicyRule.*" nested query
// parameters (field-diffed against the installed SDK's
// awsEc2query_serializeDocumentTransitGatewayRequestPolicyRule).
func policyRuleFromVals(vals url.Values) TransitGatewayPolicyTableEntry {
	return TransitGatewayPolicyTableEntry{
		SourceCidrBlock:      vals.Get("PolicyRule.SourceCidrBlock"),
		SourcePortRange:      vals.Get("PolicyRule.SourcePortRange"),
		DestinationCidrBlock: vals.Get("PolicyRule.DestinationCidrBlock"),
		DestinationPortRange: vals.Get("PolicyRule.DestinationPortRange"),
		Protocol:             vals.Get("PolicyRule.Protocol"),
		MetaDataKey:          vals.Get("PolicyRule.MetaData.MetaDataKey"),
		MetaDataValue:        vals.Get("PolicyRule.MetaData.MetaDataValue"),
	}
}

func (h *Handler) handleCreateTransitGatewayPolicyTableEntry(
	vals url.Values,
	reqID string,
) (any, error) {
	policyTableID := vals.Get("TransitGatewayPolicyTableId")
	ruleNumber, _ := strconv.Atoi(vals.Get("PolicyRuleNumber"))

	entry := policyRuleFromVals(vals)
	entry.PolicyRuleNumber = ruleNumber
	entry.TargetRouteTableID = vals.Get("TargetRouteTableId")

	stored, err := h.Backend.CreateTransitGatewayPolicyTableEntry(policyTableID, &entry)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayPolicyTableEntryResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Entry:     tgwPolicyTableEntryToItem(stored),
	}, nil
}

func (h *Handler) handleModifyTransitGatewayPolicyTableEntry(
	vals url.Values,
	reqID string,
) (any, error) {
	policyTableID := vals.Get("TransitGatewayPolicyTableId")
	ruleNumber, _ := strconv.Atoi(vals.Get("PolicyRuleNumber"))

	updates := policyRuleFromVals(vals)
	updates.TargetRouteTableID = vals.Get("TargetRouteTableId")

	entry, err := h.Backend.ModifyTransitGatewayPolicyTableEntry(policyTableID, ruleNumber, &updates)
	if err != nil {
		return nil, err
	}

	return &modifyTransitGatewayPolicyTableEntryResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Entry:     tgwPolicyTableEntryToItem(entry),
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayPolicyTableEntry(
	vals url.Values,
	reqID string,
) (any, error) {
	policyTableID := vals.Get("TransitGatewayPolicyTableId")
	ruleNumber, _ := strconv.Atoi(vals.Get("PolicyRuleNumber"))

	entry, err := h.Backend.DeleteTransitGatewayPolicyTableEntry(policyTableID, ruleNumber)
	if err != nil {
		return nil, err
	}

	return &deleteTransitGatewayPolicyTableEntryResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Entry:     tgwPolicyTableEntryToItem(entry),
	}, nil
}

// ---- Handlers: route table announcements ----

func (h *Handler) handleCreateTransitGatewayRouteTableAnnouncement(
	vals url.Values,
	reqID string,
) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	peeringAttachmentID := vals.Get("PeeringAttachmentId")

	ann, err := h.Backend.CreateTransitGatewayRouteTableAnnouncement(routeTableID, peeringAttachmentID)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "transit-gateway-route-table-announcement")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags(
			[]string{ann.TransitGatewayRouteTableAnnouncementID}, tags,
		); err != nil {
			return nil, err
		}
	}

	return &createTransitGatewayRouteTableAnnouncementResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		Announcement: tgwRouteTableAnnouncementToItem(ann, tags),
	}, nil
}

func (h *Handler) handleDeleteTransitGatewayRouteTableAnnouncement(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("TransitGatewayRouteTableAnnouncementId")

	existing := h.Backend.DescribeTransitGatewayRouteTableAnnouncements([]string{id})
	tags := h.Backend.TagsForResource(id)

	if err := h.Backend.DeleteTransitGatewayRouteTableAnnouncement(id); err != nil {
		return nil, err
	}

	var item tgwRouteTableAnnouncementItem
	if len(existing) > 0 {
		item = tgwRouteTableAnnouncementToItem(existing[0], tags)
		item.State = tgwRouteStateDeleted
	}

	return &deleteTransitGatewayRouteTableAnnouncementResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		Announcement: item,
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayRouteTableAnnouncements(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayRouteTableAnnouncementIds")
	anns := h.Backend.DescribeTransitGatewayRouteTableAnnouncements(ids)

	resp := &describeTransitGatewayRouteTableAnnouncementsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range anns {
		resp.Announcements.Items = append(
			resp.Announcements.Items,
			tgwRouteTableAnnouncementToItem(a, h.Backend.TagsForResource(a.TransitGatewayRouteTableAnnouncementID)),
		)
	}

	return resp, nil
}

// ---- Handlers: route table associations / propagations / search / export ----

func (h *Handler) handleGetTransitGatewayRouteTableAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")

	assocs, err := h.Backend.GetTransitGatewayRouteTableAssociations(routeTableID)
	if err != nil {
		return nil, err
	}

	resp := &getTransitGatewayRouteTableAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.Associations.Items = append(resp.Associations.Items, tgwRTAssociationGetItem{
			ResourceID:                 a.ResourceID,
			ResourceType:               a.ResourceType,
			State:                      a.State,
			TransitGatewayAttachmentID: a.TransitGatewayAttachmentID,
		})
	}

	return resp, nil
}

func (h *Handler) handleGetTransitGatewayRouteTablePropagations(
	vals url.Values,
	reqID string,
) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")

	props, err := h.Backend.GetTransitGatewayRouteTablePropagations(routeTableID)
	if err != nil {
		return nil, err
	}

	resp := &getTransitGatewayRouteTablePropagationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range props {
		resp.Propagations.Items = append(resp.Propagations.Items, tgwRTPropagationItem{
			ResourceID:                             p.ResourceID,
			ResourceType:                           p.ResourceType,
			State:                                  p.State,
			TransitGatewayAttachmentID:             p.TransitGatewayAttachmentID,
			TransitGatewayRouteTableAnnouncementID: p.TransitGatewayRouteTableAnnouncementID,
		})
	}

	return resp, nil
}

func (h *Handler) handleGetTransitGatewayAttachmentPropagations(
	vals url.Values,
	reqID string,
) (any, error) {
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	props, err := h.Backend.GetTransitGatewayAttachmentPropagations(attachmentID)
	if err != nil {
		return nil, err
	}

	resp := &getTransitGatewayAttachmentPropagationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, p := range props {
		resp.Propagations.Items = append(resp.Propagations.Items, tgwAttachmentPropagationItem{
			TransitGatewayRouteTableID: p.TransitGatewayRouteTableID,
			State:                      p.State,
		})
	}

	return resp, nil
}

func (h *Handler) handleSearchTransitGatewayRoutes(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	filters := parseEC2Filters(vals)

	routes, err := h.Backend.SearchTransitGatewayRoutes(routeTableID, filters)
	if err != nil {
		return nil, err
	}

	resp := &searchTransitGatewayRoutesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range routes {
		resp.RouteSet.Items = append(resp.RouteSet.Items, tgwRouteToItem(r))
	}

	return resp, nil
}

func (h *Handler) handleExportTransitGatewayRoutes(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	s3Bucket := vals.Get("S3Bucket")

	location, err := h.Backend.ExportTransitGatewayRoutes(routeTableID, s3Bucket)
	if err != nil {
		return nil, err
	}

	return &exportTransitGatewayRoutesResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		S3Location: location,
	}, nil
}

// ---- Handlers: modifications ----

func (h *Handler) handleModifyTransitGatewayVpcAttachment(vals url.Values, reqID string) (any, error) {
	attachmentID := vals.Get("TransitGatewayAttachmentId")
	addSubnetIDs := parseMemberList(vals, "AddSubnetIds")
	removeSubnetIDs := parseMemberList(vals, "RemoveSubnetIds")

	att, err := h.Backend.ModifyTransitGatewayVpcAttachment(attachmentID, addSubnetIDs, removeSubnetIDs)
	if err != nil {
		return nil, err
	}

	return &modifyTransitGatewayVpcAttachmentResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		Attachment: tgwVpcAttachmentToItem(att, h.Backend.TagsForResource(att.TransitGatewayAttachmentID)),
	}, nil
}

func (h *Handler) handleModifyTransitGatewayMeteringPolicy(vals url.Values, reqID string) (any, error) {
	policyID := vals.Get("TransitGatewayMeteringPolicyId")
	addIDs := parseMemberList(vals, "AddMiddleboxAttachmentIds")
	removeIDs := parseMemberList(vals, "RemoveMiddleboxAttachmentIds")

	policy, err := h.Backend.ModifyTransitGatewayMeteringPolicy(policyID, addIDs, removeIDs)
	if err != nil {
		return nil, err
	}

	return &modifyTransitGatewayMeteringPolicyResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Policy:    tgwMeteringPolicyToItem(policy),
	}, nil
}

func (h *Handler) handleGetTransitGatewayMeteringPolicyEntries(
	vals url.Values,
	reqID string,
) (any, error) {
	policyID := vals.Get("TransitGatewayMeteringPolicyId")

	entries, err := h.Backend.GetTransitGatewayMeteringPolicyEntries(policyID)
	if err != nil {
		return nil, err
	}

	resp := &getTransitGatewayMeteringPolicyEntriesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, e := range entries {
		resp.Entries.Items = append(resp.Entries.Items, tgwMeteringPolicyEntryToItem(e))
	}

	return resp, nil
}

func (h *Handler) handleModifyTransitGatewayPrefixListReference(
	vals url.Values,
	reqID string,
) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	prefixListID := vals.Get("PrefixListId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")
	blackhole := vals.Get("Blackhole") == ec2BooleanTrue

	ref, err := h.Backend.ModifyTransitGatewayPrefixListReference(
		routeTableID,
		prefixListID,
		attachmentID,
		blackhole,
	)
	if err != nil {
		return nil, err
	}

	return &modifyTransitGatewayPrefixListReferenceResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Reference: tgwPrefixListRefToItem(ref),
	}, nil
}

// ---- Handlers: rejections ----

func (h *Handler) handleRejectTransitGatewayVpcAttachment(vals url.Values, reqID string) (any, error) {
	att, err := h.Backend.RejectTransitGatewayVpcAttachment(vals.Get("TransitGatewayAttachmentId"))
	if err != nil {
		return nil, err
	}

	return &rejectTransitGatewayVpcAttachmentResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		Attachment: tgwVpcAttachmentToItem(att, h.Backend.TagsForResource(att.TransitGatewayAttachmentID)),
	}, nil
}

func (h *Handler) handleRejectTransitGatewayPeeringAttachment(
	vals url.Values,
	reqID string,
) (any, error) {
	att, err := h.Backend.RejectTransitGatewayPeeringAttachment(vals.Get("TransitGatewayAttachmentId"))
	if err != nil {
		return nil, err
	}

	return &rejectTransitGatewayPeeringAttachmentResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Attachment: toTGWPeeringAttachmentItem(
			att, h.Backend.TagsForResource(att.TransitGatewayAttachmentID),
		),
	}, nil
}

func (h *Handler) handleRejectTransitGatewayMulticastDomainAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")
	subnetIDs := parseMemberList(vals, "SubnetIds")

	assocs, err := h.Backend.RejectTransitGatewayMulticastDomainAssociations(
		domainID,
		attachmentID,
		subnetIDs,
	)
	if err != nil {
		return nil, err
	}

	resp := &rejectTransitGatewayMulticastDomainAssociationsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, a := range assocs {
		resp.Associations.Items = append(resp.Associations.Items, tgwMulticastDomainAssociationItem{
			TransitGatewayMulticastDomainID: a.TransitGatewayMulticastDomainID,
			TransitGatewayAttachmentID:      a.TransitGatewayAttachmentID,
			SubnetID:                        a.SubnetID,
			State:                           a.State,
		})
	}

	return resp, nil
}
