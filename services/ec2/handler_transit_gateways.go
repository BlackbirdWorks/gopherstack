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
	XMLName   xml.Name `xml:"DeleteTransitGatewayResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// ---- Handler implementations ----

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
