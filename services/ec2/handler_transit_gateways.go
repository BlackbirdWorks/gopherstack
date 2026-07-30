package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleEnableVgwRoutePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("RouteTableId")
	gatewayID := vals.Get("GatewayId")
	if err := h.Backend.EnableVgwRoutePropagation(routeTableID, gatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "EnableVgwRoutePropagationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDisableVgwRoutePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("RouteTableId")
	gatewayID := vals.Get("GatewayId")
	if err := h.Backend.DisableVgwRoutePropagation(routeTableID, gatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisableVgwRoutePropagationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleModifyTransitGateway(vals url.Values, reqID string) (any, error) {
	tgwID := vals.Get("TransitGatewayId")
	description := vals.Get("Description")
	if err := h.Backend.ModifyTransitGateway(tgwID, description); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ModifyTransitGatewayResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
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
	XMLName        xml.Name           `xml:"DeleteTransitGatewayResponse"`
	RequestID      string             `xml:"requestId"`
	TransitGateway transitGatewayItem `xml:"transitGateway"`
}

// ---- Handler implementations ----

func (h *Handler) handleDescribeTransitGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayIds")

	tgws := h.Backend.DescribeTransitGateways(ids)
	resp := &describeTransitGatewaysResponse{RequestID: reqID}

	for _, tgw := range tgws {
		resp.TransitGatewaySet.Items = append(
			resp.TransitGatewaySet.Items, toTransitGatewayItem(tgw, h.Backend.TagsForResource(tgw.ID)),
		)
	}

	return resp, nil
}

// parseTransitGatewayRequestOptions extracts the Options.* fields accepted by
// CreateTransitGateway, matching the real request's Options.<Field> query
// parameter names.
func parseTransitGatewayRequestOptions(vals url.Values) (int64, []string) {
	var amazonSideAsn int64
	if v := vals.Get("Options.AmazonSideAsn"); v != "" {
		amazonSideAsn, _ = strconv.ParseInt(v, 10, 64)
	}

	cidrBlocks := parseMemberList(vals, "Options.TransitGatewayCidrBlocks")

	return amazonSideAsn, cidrBlocks
}

func (h *Handler) handleCreateTransitGateway(vals url.Values, reqID string) (any, error) {
	amazonSideAsn, cidrBlocks := parseTransitGatewayRequestOptions(vals)

	tgw, err := h.Backend.CreateTransitGateway(CreateTransitGatewayParams{
		Description:                     vals.Get("Description"),
		AmazonSideAsn:                   amazonSideAsn,
		AutoAcceptSharedAttachments:     vals.Get("Options.AutoAcceptSharedAttachments"),
		DefaultRouteTableAssociation:    vals.Get("Options.DefaultRouteTableAssociation"),
		DefaultRouteTablePropagation:    vals.Get("Options.DefaultRouteTablePropagation"),
		DNSSupport:                      vals.Get("Options.DnsSupport"),
		MulticastSupport:                vals.Get("Options.MulticastSupport"),
		SecurityGroupReferencingSupport: vals.Get("Options.SecurityGroupReferencingSupport"),
		VpnEcmpSupport:                  vals.Get("Options.VpnEcmpSupport"),
		TransitGatewayCidrBlocks:        cidrBlocks,
		Tags:                            parseTagSpecification(vals, "transit-gateway"),
	})
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayResponse{
		RequestID:      reqID,
		TransitGateway: toTransitGatewayItem(tgw, h.Backend.TagsForResource(tgw.ID)),
	}, nil
}

func (h *Handler) handleDeleteTransitGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("TransitGatewayId")
	tags := h.Backend.TagsForResource(id)

	tgw, err := h.Backend.DeleteTransitGateway(id)
	if err != nil {
		return nil, err
	}

	return &deleteTransitGatewayResponse{
		RequestID:      reqID,
		TransitGateway: toTransitGatewayItem(tgw, tags),
	}, nil
}

func toTGWRouteTablePropagationItem(p *TransitGatewayRouteTablePropagation) tgwRouteTablePropagationItem {
	return tgwRouteTablePropagationItem{
		ResourceID:                             p.ResourceID,
		ResourceType:                           p.ResourceType,
		State:                                  p.State,
		TransitGatewayAttachmentID:             p.TransitGatewayAttachmentID,
		TransitGatewayRouteTableAnnouncementID: p.TransitGatewayRouteTableAnnouncementID,
	}
}

type enableTransitGatewayRouteTablePropagationResponse struct {
	XMLName     xml.Name                     `xml:"EnableTransitGatewayRouteTablePropagationResponse"`
	RequestID   string                       `xml:"requestId"`
	Propagation tgwRouteTablePropagationItem `xml:"propagation"`
}

func (h *Handler) handleEnableTransitGatewayRouteTablePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	prop, err := h.Backend.EnableTransitGatewayRouteTablePropagation(routeTableID, attachmentID)
	if err != nil {
		return nil, err
	}

	return &enableTransitGatewayRouteTablePropagationResponse{
		RequestID:   reqID,
		Propagation: toTGWRouteTablePropagationItem(prop),
	}, nil
}

type disableTransitGatewayRouteTablePropagationResponse struct {
	XMLName     xml.Name                     `xml:"DisableTransitGatewayRouteTablePropagationResponse"`
	RequestID   string                       `xml:"requestId"`
	Propagation tgwRouteTablePropagationItem `xml:"propagation"`
}

func (h *Handler) handleDisableTransitGatewayRouteTablePropagation(vals url.Values, reqID string) (any, error) {
	routeTableID := vals.Get("TransitGatewayRouteTableId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	prop, err := h.Backend.DisableTransitGatewayRouteTablePropagation(routeTableID, attachmentID)
	if err != nil {
		return nil, err
	}

	return &disableTransitGatewayRouteTablePropagationResponse{
		RequestID:   reqID,
		Propagation: toTGWRouteTablePropagationItem(prop),
	}, nil
}

type tgwAttachmentSummaryItem struct {
	TransitGatewayAttachmentID string `xml:"transitGatewayAttachmentId"`
	TransitGatewayID           string `xml:"transitGatewayId,omitempty"`
	ResourceID                 string `xml:"resourceId,omitempty"`
	ResourceType               string `xml:"resourceType,omitempty"`
	State                      string `xml:"state"`
}

type describeTransitGatewayAttachmentsResponse struct {
	XMLName     xml.Name `xml:"DescribeTransitGatewayAttachmentsResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	Attachments struct {
		Items []tgwAttachmentSummaryItem `xml:"item"`
	} `xml:"transitGatewayAttachments"`
}

func (h *Handler) handleDescribeTransitGatewayAttachments(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayAttachmentId")

	atts := h.Backend.DescribeTransitGatewayAttachments(ids)

	resp := &describeTransitGatewayAttachmentsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, att := range atts {
		resp.Attachments.Items = append(resp.Attachments.Items, tgwAttachmentSummaryItem{
			TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
			TransitGatewayID:           att.TransitGatewayID,
			ResourceID:                 att.ResourceID,
			ResourceType:               att.ResourceType,
			State:                      att.State,
		})
	}

	return resp, nil
}

// ---- Capacity Reservation extras ----

// registerTransitGatewaysOps registers the TransitGateways operation handlers.
func registerTransitGatewaysOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["EnableVgwRoutePropagation"] = h.handleEnableVgwRoutePropagation
	ops["DisableVgwRoutePropagation"] = h.handleDisableVgwRoutePropagation
	ops["ModifyTransitGateway"] = h.handleModifyTransitGateway
	ops["DescribeTransitGateways"] = h.handleDescribeTransitGateways
	ops["CreateTransitGateway"] = h.handleCreateTransitGateway
	ops["DeleteTransitGateway"] = h.handleDeleteTransitGateway
	ops["EnableTransitGatewayRouteTablePropagation"] = h.handleEnableTransitGatewayRouteTablePropagation
	ops["DisableTransitGatewayRouteTablePropagation"] = h.handleDisableTransitGatewayRouteTablePropagation
	ops["DescribeTransitGatewayAttachments"] = h.handleDescribeTransitGatewayAttachments
}

// transitGatewaysSupportedOperations lists the operation names registered by
// registerTransitGatewaysOps, for GetSupportedOperations().
func transitGatewaysSupportedOperations() []string {
	return []string{
		"EnableVgwRoutePropagation",
		"DisableVgwRoutePropagation",
		"ModifyTransitGateway",
		"DescribeTransitGateways",
		"CreateTransitGateway",
		"DeleteTransitGateway",
		"EnableTransitGatewayRouteTablePropagation",
		"DisableTransitGatewayRouteTablePropagation",
		"DescribeTransitGatewayAttachments",
	}
}
