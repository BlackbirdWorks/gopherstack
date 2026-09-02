package ec2

import (
	"net/url"
)

// ---- VPN Gateway handlers ----

func (h *Handler) handleCreateVpnGateway(vals url.Values, reqID string) (any, error) {
	vgw, err := h.Backend.CreateVpnGateway(vals.Get("Type"))
	if err != nil {
		return nil, err
	}

	item := vpnGatewayItem{
		VpnGatewayID:    vgw.VpnGatewayID,
		State:           vgw.State,
		Type:            vgw.Type,
		AttachedVPCID:   vgw.AttachedVPCID,
		AttachmentState: vgw.AttachmentState,
		TagSet:          tagItemsFromMap(h.Backend.TagsForResource(vgw.VpnGatewayID)),
	}

	return &createVpnGatewayResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		VpnGateway: item,
	}, nil
}

// handleDescribeVpnGateways previously never read Filters at all
// (awsEc2query_serializeOpDocumentDescribeVpnGatewaysInput declares it) --
// see applyVpnGatewayFilters (handler_filters.go).
func (h *Handler) handleDescribeVpnGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpnGatewayId")
	vgws := applyVpnGatewayFilters(h.Backend.DescribeVpnGateways(ids), parseEC2Filters(vals), h.Backend)

	resp := &describeVpnGatewaysResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, vgw := range vgws {
		resp.VpnGatewaySet.Items = append(resp.VpnGatewaySet.Items, vpnGatewayItem{
			VpnGatewayID:    vgw.VpnGatewayID,
			State:           vgw.State,
			Type:            vgw.Type,
			AttachedVPCID:   vgw.AttachedVPCID,
			AttachmentState: vgw.AttachmentState,
			TagSet:          tagItemsFromMap(h.Backend.TagsForResource(vgw.VpnGatewayID)),
		})
	}

	return resp, nil
}

func (h *Handler) handleDeleteVpnGateway(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteVpnGateway(vals.Get("VpnGatewayId")); err != nil {
		return nil, err
	}

	return &deleteVpnGatewayResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleAttachVpnGateway(vals url.Values, reqID string) (any, error) {
	vgwID := vals.Get("VpnGatewayId")
	vpcID := vals.Get("VpcId")

	if err := h.Backend.AttachVpnGateway(vgwID, vpcID); err != nil {
		return nil, err
	}

	return &attachVpnGatewayResponse{
		RequestID:       reqID,
		AttachmentState: attachmentStateAttached,
		VpcID:           vpcID,
	}, nil
}

func (h *Handler) handleDetachVpnGateway(vals url.Values, reqID string) (any, error) {
	vgwID := vals.Get("VpnGatewayId")
	vpcID := vals.Get("VpcId")

	if err := h.Backend.DetachVpnGateway(vgwID, vpcID); err != nil {
		return nil, err
	}

	return &detachVpnGatewayResponse{RequestID: reqID, Return: true}, nil
}

// ---- Customer Gateway handlers ----

func (h *Handler) handleCreateCustomerGateway(vals url.Values, reqID string) (any, error) {
	cgw, err := h.Backend.CreateCustomerGateway(
		vals.Get("Type"),
		vals.Get("IpAddress"),
		vals.Get("BgpAsn"),
	)
	if err != nil {
		return nil, err
	}

	return &createCustomerGatewayResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		CustomerGateway: customerGatewayItem{
			CustomerGatewayID: cgw.CustomerGatewayID,
			State:             cgw.State,
			Type:              cgw.Type,
			BgpAsn:            cgw.BgpAsn,
			IPAddress:         cgw.IPAddress,
			TagSet:            tagItemsFromMap(h.Backend.TagsForResource(cgw.CustomerGatewayID)),
		},
	}, nil
}

// handleDescribeCustomerGateways previously never read Filters at all
// (awsEc2query_serializeOpDocumentDescribeCustomerGatewaysInput declares
// it) -- see applyCustomerGatewayFilters (handler_filters.go).
func (h *Handler) handleDescribeCustomerGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CustomerGatewayId")
	cgws := applyCustomerGatewayFilters(h.Backend.DescribeCustomerGateways(ids), parseEC2Filters(vals), h.Backend)

	resp := &describeCustomerGatewaysResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, cgw := range cgws {
		resp.CustomerGatewaySet.Items = append(resp.CustomerGatewaySet.Items, customerGatewayItem{
			CustomerGatewayID: cgw.CustomerGatewayID,
			State:             cgw.State,
			Type:              cgw.Type,
			BgpAsn:            cgw.BgpAsn,
			IPAddress:         cgw.IPAddress,
			TagSet:            tagItemsFromMap(h.Backend.TagsForResource(cgw.CustomerGatewayID)),
		})
	}

	return resp, nil
}

func (h *Handler) handleDeleteCustomerGateway(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteCustomerGateway(vals.Get("CustomerGatewayId")); err != nil {
		return nil, err
	}

	return &deleteCustomerGatewayResponse{RequestID: reqID, Return: true}, nil
}
