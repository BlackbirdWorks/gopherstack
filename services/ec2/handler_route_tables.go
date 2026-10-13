package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleReplaceRoute(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.ReplaceRouteSpec(vals.Get("RouteTableId"), routeSpecFromVals(vals)); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ReplaceRouteResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

// registerRouteTablesOps registers the RouteTables operation handlers.
func registerRouteTablesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["ReplaceRoute"] = h.handleReplaceRoute
}

// routeTablesSupportedOperations lists the operation names registered by
// registerRouteTablesOps, for GetSupportedOperations().
func routeTablesSupportedOperations() []string {
	return []string{
		"ReplaceRoute",
	}
}

type routeItem struct {
	DestinationCIDR             string `xml:"destinationCidrBlock,omitempty"`
	DestinationIPv6CIDR         string `xml:"destinationIpv6CidrBlock,omitempty"`
	DestinationPrefixListID     string `xml:"destinationPrefixListId,omitempty"`
	GatewayID                   string `xml:"gatewayId,omitempty"`
	NatGatewayID                string `xml:"natGatewayId,omitempty"`
	InstanceID                  string `xml:"instanceId,omitempty"`
	NetworkInterfaceID          string `xml:"networkInterfaceId,omitempty"`
	VpcPeeringConnectionID      string `xml:"vpcPeeringConnectionId,omitempty"`
	TransitGatewayID            string `xml:"transitGatewayId,omitempty"`
	EgressOnlyInternetGatewayID string `xml:"egressOnlyInternetGatewayId,omitempty"`
	LocalGatewayID              string `xml:"localGatewayId,omitempty"`
	CarrierGatewayID            string `xml:"carrierGatewayId,omitempty"`
	CoreNetworkArn              string `xml:"coreNetworkArn,omitempty"`
	Origin                      string `xml:"origin,omitempty"`
	State                       string `xml:"state"`
}

func routeSpecFromVals(vals url.Values) RouteSpec {
	localTarget, _ := strconv.ParseBool(vals.Get("LocalTarget"))

	return RouteSpec{
		DestinationCIDR:             vals.Get("DestinationCidrBlock"),
		DestinationIPv6CIDR:         vals.Get("DestinationIpv6CidrBlock"),
		DestinationPrefixListID:     vals.Get("DestinationPrefixListId"),
		GatewayID:                   vals.Get("GatewayId"),
		NatGatewayID:                vals.Get("NatGatewayId"),
		InstanceID:                  vals.Get("InstanceId"),
		NetworkInterfaceID:          vals.Get("NetworkInterfaceId"),
		VpcPeeringConnectionID:      vals.Get("VpcPeeringConnectionId"),
		TransitGatewayID:            vals.Get("TransitGatewayId"),
		EgressOnlyInternetGatewayID: vals.Get("EgressOnlyInternetGatewayId"),
		VpcEndpointID:               vals.Get("VpcEndpointId"),
		LocalGatewayID:              vals.Get("LocalGatewayId"),
		CarrierGatewayID:            vals.Get("CarrierGatewayId"),
		CoreNetworkArn:              vals.Get("CoreNetworkArn"),
		LocalTarget:                 localTarget,
	}
}

type routeSet struct {
	Items []routeItem `xml:"item"`
}

type assocItem struct {
	RouteTableAssociationID string `xml:"routeTableAssociationId"`
	RouteTableID            string `xml:"routeTableId"`
	SubnetID                string `xml:"subnetId,omitempty"`
	Main                    bool   `xml:"main"`
}

type assocSet struct {
	Items []assocItem `xml:"item"`
}

type propagatingVgwItem struct {
	GatewayID string `xml:"gatewayId"`
}

type routeTableItem struct {
	RouteTableID      string               `xml:"routeTableId"`
	VPCID             string               `xml:"vpcId"`
	RouteSet          routeSet             `xml:"routeSet"`
	AssociationSet    assocSet             `xml:"associationSet"`
	PropagatingVgwSet []propagatingVgwItem `xml:"propagatingVgwSet>item"`
	TagSet            []simpleTagItem      `xml:"tagSet>item"`
}

type routeTableItemSet struct {
	Items []routeTableItem `xml:"item"`
}

type describeRouteTablesResponse struct {
	XMLName       xml.Name          `xml:"DescribeRouteTablesResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	NextToken     string            `xml:"nextToken,omitempty"`
	RouteTableSet routeTableItemSet `xml:"routeTableSet"`
}

type createRouteTableResponse struct {
	XMLName    xml.Name       `xml:"CreateRouteTableResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	RouteTable routeTableItem `xml:"routeTable"`
}

type deleteRouteTableResponse struct {
	XMLName   xml.Name `xml:"DeleteRouteTableResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type createRouteResponse struct {
	XMLName   xml.Name `xml:"CreateRouteResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type deleteRouteResponse struct {
	XMLName   xml.Name `xml:"DeleteRouteResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type associateRouteTableResponse struct {
	XMLName       xml.Name `xml:"AssociateRouteTableResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	RequestID     string   `xml:"requestId"`
	AssociationID string   `xml:"associationId"`
}

type disassociateRouteTableResponse struct {
	XMLName   xml.Name `xml:"DisassociateRouteTableResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func toRouteTableItem(rt *RouteTable, tags map[string]string, propagatingVgws []string) routeTableItem {
	routes := make([]routeItem, 0, len(rt.Routes))
	for _, r := range rt.Routes {
		routes = append(routes, routeItem{
			DestinationCIDR:             r.DestinationCIDR,
			DestinationIPv6CIDR:         r.DestinationIPv6CIDR,
			DestinationPrefixListID:     r.DestinationPrefixListID,
			GatewayID:                   r.GatewayID,
			NatGatewayID:                r.NatGatewayID,
			InstanceID:                  r.InstanceID,
			NetworkInterfaceID:          r.NetworkInterfaceID,
			VpcPeeringConnectionID:      r.VpcPeeringConnectionID,
			TransitGatewayID:            r.TransitGatewayID,
			EgressOnlyInternetGatewayID: r.EgressOnlyInternetGatewayID,
			LocalGatewayID:              r.LocalGatewayID,
			CarrierGatewayID:            r.CarrierGatewayID,
			CoreNetworkArn:              r.CoreNetworkArn,
			Origin:                      r.Origin,
			State:                       r.State,
		})
	}

	assocs := make([]assocItem, 0, len(rt.Associations))
	for _, a := range rt.Associations {
		assocs = append(assocs, assocItem{
			RouteTableAssociationID: a.ID,
			RouteTableID:            a.RouteTableID,
			SubnetID:                a.SubnetID,
			Main:                    a.Main,
		})
	}

	vgws := make([]propagatingVgwItem, 0, len(propagatingVgws))
	for _, gw := range propagatingVgws {
		vgws = append(vgws, propagatingVgwItem{GatewayID: gw})
	}

	return routeTableItem{
		RouteTableID:      rt.ID,
		VPCID:             rt.VPCID,
		RouteSet:          routeSet{Items: routes},
		AssociationSet:    assocSet{Items: assocs},
		PropagatingVgwSet: vgws,
		TagSet:            tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateRouteTable(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	rt, err := h.Backend.CreateRouteTable(vpcID)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "route-table")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{rt.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createRouteTableResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		RouteTable: toRouteTableItem(rt, tags, h.Backend.GetPropagatingVgws(rt.ID)),
	}, nil
}

func (h *Handler) handleDeleteRouteTable(vals url.Values, reqID string) (any, error) {
	id := vals.Get("RouteTableId")
	if id == "" {
		return nil, fmt.Errorf("%w: RouteTableId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteRouteTable(id); err != nil {
		return nil, err
	}

	return &deleteRouteTableResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeRouteTables(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "RouteTableId")

	rts, err := h.Backend.DescribeRouteTables(ids)
	if err != nil {
		return nil, err
	}

	filters := parseEC2Filters(vals)
	rts = applyRouteTableFilters(rts, filters, h.Backend)

	items := make([]routeTableItem, 0, len(rts))
	for _, rt := range rts {
		items = append(
			items,
			toRouteTableItem(rt, h.Backend.TagsForResource(rt.ID), h.Backend.GetPropagatingVgws(rt.ID)),
		)
	}

	return finishPaged(vals, &describeRouteTablesResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		RouteTableSet: routeTableItemSet{Items: items},
	})
}

func (h *Handler) handleCreateRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	if rtID == "" {
		return nil, fmt.Errorf("%w: RouteTableId is required", ErrInvalidParameter)
	}

	if err := h.Backend.CreateRouteSpec(rtID, routeSpecFromVals(vals)); err != nil {
		return nil, err
	}

	return &createRouteResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDeleteRoute(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	if rtID == "" {
		return nil, fmt.Errorf("%w: RouteTableId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteRouteSpec(rtID, routeSpecFromVals(vals)); err != nil {
		return nil, err
	}

	return &deleteRouteResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssociateRouteTable(vals url.Values, reqID string) (any, error) {
	rtID := vals.Get("RouteTableId")
	subnetID := vals.Get("SubnetId")

	if rtID == "" || subnetID == "" {
		return nil, fmt.Errorf("%w: RouteTableId and SubnetId are required", ErrInvalidParameter)
	}

	assocID, err := h.Backend.AssociateRouteTable(rtID, subnetID)
	if err != nil {
		return nil, err
	}

	return &associateRouteTableResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		AssociationID: assocID,
	}, nil
}

func (h *Handler) handleDisassociateRouteTable(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	if assocID == "" {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateRouteTable(assocID); err != nil {
		return nil, err
	}

	return &disassociateRouteTableResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}
