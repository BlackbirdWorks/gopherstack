package networkmanager

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// attachmentsRoutes wires PARITY.md families Q, Q1-Q5 (16 ops). Split
// across one helper per sub-family to keep this function's own length
// under funlen's limit.
func (h *Handler) attachmentsRoutes() []route {
	return concatRoutes(h.attachmentLifecycleRoutes(), h.attachmentSubtypeRoutes())
}

// attachmentLifecycleRoutes wires family Q -- the generic Accept/Reject/
// Delete/List lifecycle shared by all 5 attachment subtypes.
func (h *Handler) attachmentLifecycleRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segAttachments, paramAttachmentID, "accept"},
			op:      "AcceptAttachment",
			fn:      h.dispatchAcceptAttachment,
		},
		{
			method:  http.MethodPost,
			pattern: []string{segAttachments, paramAttachmentID, "reject"},
			op:      "RejectAttachment",
			fn:      h.dispatchRejectAttachment,
		},
		{
			method:  http.MethodDelete,
			pattern: []string{segAttachments, paramAttachmentID},
			op:      "DeleteAttachment",
			fn:      h.dispatchDeleteAttachment,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segAttachments},
			op:      "ListAttachments",
			fn:      h.dispatchListAttachments,
		},
	}
}

// attachmentSubtypeRoutes wires families Q1-Q5 -- the 5 subtype Create/Get/
// Update ops (12 ops).
func (h *Handler) attachmentSubtypeRoutes() []route {
	return []route{
		{
			method:  http.MethodPost,
			pattern: []string{segVpcAttachments},
			op:      "CreateVpcAttachment",
			fn:      h.dispatchCreateVpcAttachment,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segVpcAttachments, paramAttachmentID},
			op:      "GetVpcAttachment",
			fn:      h.dispatchGetVpcAttachment,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segVpcAttachments, paramAttachmentID},
			op:      "UpdateVpcAttachment",
			fn:      h.dispatchUpdateVpcAttachment,
		},

		{
			method:  http.MethodPost,
			pattern: []string{"connect-attachments"},
			op:      "CreateConnectAttachment",
			fn:      h.dispatchCreateConnectAttachment,
		},
		{
			method:  http.MethodGet,
			pattern: []string{"connect-attachments", paramAttachmentID},
			op:      "GetConnectAttachment",
			fn:      h.dispatchGetConnectAttachment,
		},

		{
			method:  http.MethodPost,
			pattern: []string{"site-to-site-vpn-attachments"},
			op:      "CreateSiteToSiteVpnAttachment",
			fn:      h.dispatchCreateSiteToSiteVpnAttachment,
		},
		{
			method:  http.MethodGet,
			pattern: []string{"site-to-site-vpn-attachments", paramAttachmentID},
			op:      "GetSiteToSiteVpnAttachment",
			fn:      h.dispatchGetSiteToSiteVpnAttachment,
		},

		{
			method:  http.MethodPost,
			pattern: []string{segDirectConnectGWs},
			op:      "CreateDirectConnectGatewayAttachment",
			fn:      h.dispatchCreateDirectConnectGatewayAttachment,
		},
		{
			method:  http.MethodGet,
			pattern: []string{segDirectConnectGWs, paramAttachmentID},
			op:      "GetDirectConnectGatewayAttachment",
			fn:      h.dispatchGetDirectConnectGatewayAttachment,
		},
		{
			method:  http.MethodPatch,
			pattern: []string{segDirectConnectGWs, paramAttachmentID},
			op:      "UpdateDirectConnectGatewayAttachment",
			fn:      h.dispatchUpdateDirectConnectGatewayAttachment,
		},

		{
			method:  http.MethodPost,
			pattern: []string{"transit-gateway-route-table-attachments"},
			op:      "CreateTransitGatewayRouteTableAttachment",
			fn:      h.dispatchCreateTransitGatewayRouteTableAttachment,
		},
		{
			method:  http.MethodGet,
			pattern: []string{"transit-gateway-route-table-attachments", paramAttachmentID},
			op:      "GetTransitGatewayRouteTableAttachment",
			fn:      h.dispatchGetTransitGatewayRouteTableAttachment,
		},
	}
}

// ---- Generic lifecycle (family Q) ----

func (h *Handler) dispatchAcceptAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.AcceptAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(attachmentEnvelope{Attachment: toAttachmentWire(a)})
}

func (h *Handler) dispatchRejectAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.RejectAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(attachmentEnvelope{Attachment: toAttachmentWire(a)})
}

func (h *Handler) dispatchDeleteAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.DeleteAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(attachmentEnvelope{Attachment: toAttachmentWire(a)})
}

func (h *Handler) dispatchListAttachments(_ context.Context, r *http.Request, _ routeParams, _ []byte) ([]byte, error) {
	q := r.URL.Query()
	p := h.Backend.ListAttachments(
		q.Get("attachmentType"), q.Get("coreNetworkId"), q.Get("edgeLocation"), q.Get("state"),
		queryNextToken(q), queryMaxResults(q),
	)

	out := make([]attachmentWire, len(p.Data))
	for i, a := range p.Data {
		out[i] = *toAttachmentWire(a)
	}

	return marshalResponse(listAttachmentsResponse{Attachments: out, NextToken: p.Next})
}

// ---- Q1: VPC Attachment ----

func (h *Handler) dispatchCreateVpcAttachment(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createVpcAttachmentReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateVpcAttachment(
		req.CoreNetworkID, req.VpcArn, req.SubnetArns, fromVpcOptionsWire(req.Options), req.RoutingPolicyLabel,
		tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(vpcAttachmentEnvelope{VpcAttachment: toVpcAttachmentWire(a)})
}

func (h *Handler) dispatchGetVpcAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.GetVpcAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(vpcAttachmentEnvelope{VpcAttachment: toVpcAttachmentWire(a)})
}

func (h *Handler) dispatchUpdateVpcAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateVpcAttachmentReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.UpdateVpcAttachment(
		params["AttachmentId"], req.AddSubnetArns, req.RemoveSubnetArns, fromVpcOptionsWire(req.Options),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(vpcAttachmentEnvelope{VpcAttachment: toVpcAttachmentWire(a)})
}

// ---- Q2: Connect Attachment ----

func (h *Handler) dispatchCreateConnectAttachment(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createConnectAttachmentReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	protocol := ""
	if req.Options != nil {
		protocol = req.Options.Protocol
	}

	a, err := h.Backend.CreateConnectAttachment(
		req.CoreNetworkID, req.EdgeLocation, req.TransportAttachmentID, protocol, req.RoutingPolicyLabel,
		tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectAttachmentEnvelope{ConnectAttachment: toConnectAttachmentWire(a)})
}

func (h *Handler) dispatchGetConnectAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.GetConnectAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(connectAttachmentEnvelope{ConnectAttachment: toConnectAttachmentWire(a)})
}

// ---- Q3: Site-to-Site VPN Attachment ----

func (h *Handler) dispatchCreateSiteToSiteVpnAttachment(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createSiteToSiteVpnAttachmentReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateSiteToSiteVpnAttachment(
		req.CoreNetworkID,
		req.VpnConnectionArn,
		req.RoutingPolicyLabel,
		tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(siteToSiteVpnAttachmentEnvelope{SiteToSiteVpnAttachment: toSiteToSiteVpnAttachmentWire(a)})
}

func (h *Handler) dispatchGetSiteToSiteVpnAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.GetSiteToSiteVpnAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(siteToSiteVpnAttachmentEnvelope{SiteToSiteVpnAttachment: toSiteToSiteVpnAttachmentWire(a)})
}

// ---- Q4: Direct Connect Gateway Attachment ----

func (h *Handler) dispatchCreateDirectConnectGatewayAttachment(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createDirectConnectGatewayAttachmentReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateDirectConnectGatewayAttachment(
		req.CoreNetworkID,
		req.DirectConnectGatewayArn,
		req.EdgeLocations,
		req.RoutingPolicyLabel,
		tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		directConnectGatewayAttachmentEnvelope{DirectConnectGatewayAttachment: toDirectConnectGatewayAttachmentWire(a)},
	)
}

func (h *Handler) dispatchGetDirectConnectGatewayAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.GetDirectConnectGatewayAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		directConnectGatewayAttachmentEnvelope{DirectConnectGatewayAttachment: toDirectConnectGatewayAttachmentWire(a)},
	)
}

func (h *Handler) dispatchUpdateDirectConnectGatewayAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	body []byte,
) ([]byte, error) {
	var req updateDirectConnectGatewayAttachmentReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.UpdateDirectConnectGatewayAttachment(params["AttachmentId"], req.EdgeLocations)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		directConnectGatewayAttachmentEnvelope{DirectConnectGatewayAttachment: toDirectConnectGatewayAttachmentWire(a)},
	)
}

// ---- Q5: Transit Gateway Route Table Attachment ----

func (h *Handler) dispatchCreateTransitGatewayRouteTableAttachment(
	_ context.Context,
	_ *http.Request,
	_ routeParams,
	body []byte,
) ([]byte, error) {
	var req createTransitGatewayRouteTableAttachmentReq
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateTransitGatewayRouteTableAttachment(
		req.PeeringID, req.TransitGatewayRouteTableArn, req.RoutingPolicyLabel, tags.MapFromKV(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		transitGatewayRouteTableAttachmentEnvelope{
			TransitGatewayRouteTableAttachment: toTransitGatewayRouteTableAttachmentWire(a),
		},
	)
}

func (h *Handler) dispatchGetTransitGatewayRouteTableAttachment(
	_ context.Context,
	_ *http.Request,
	params routeParams,
	_ []byte,
) ([]byte, error) {
	a, err := h.Backend.GetTransitGatewayRouteTableAttachment(params["AttachmentId"])
	if err != nil {
		return nil, err
	}

	return marshalResponse(
		transitGatewayRouteTableAttachmentEnvelope{
			TransitGatewayRouteTableAttachment: toTransitGatewayRouteTableAttachmentWire(a),
		},
	)
}
