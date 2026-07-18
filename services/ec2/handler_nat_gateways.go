package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleDisassociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	if err := h.Backend.DisassociateNatGatewayAddress(natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	allocationID := vals.Get("AllocationId")
	if err := h.Backend.AssociateNatGatewayAddress(natGatewayID, allocationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AssociateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAssignPrivateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	if err := h.Backend.AssignPrivateNatGatewayAddress(natGatewayID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "AssignPrivateNatGatewayAddressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type unassignPrivateNatGatewayAddressResponse struct {
	XMLName             xml.Name             `xml:"UnassignPrivateNatGatewayAddressResponse"`
	Xmlns               string               `xml:"xmlns,attr"`
	RequestID           string               `xml:"requestId"`
	NatGatewayID        string               `xml:"natGatewayId,omitempty"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
}

func (h *Handler) handleUnassignPrivateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	privateIPs := parseMemberList(vals, "PrivateIpAddress")

	ngw, err := h.Backend.UnassignPrivateNatGatewayAddress(natGatewayID, privateIPs)
	if err != nil {
		return nil, err
	}

	item := toNatGatewayItem(ngw)

	return &unassignPrivateNatGatewayAddressResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NatGatewayID:        ngw.ID,
		NatGatewayAddresses: item.NatGatewayAddresses,
	}, nil
}

// ---- Image extras ----

// registerNatGatewaysOps registers the NatGateways operation handlers.
func registerNatGatewaysOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DisassociateNatGatewayAddress"] = h.handleDisassociateNatGatewayAddress
	ops["AssociateNatGatewayAddress"] = h.handleAssociateNatGatewayAddress
	ops["AssignPrivateNatGatewayAddress"] = h.handleAssignPrivateNatGatewayAddress
	ops["UnassignPrivateNatGatewayAddress"] = h.handleUnassignPrivateNatGatewayAddress
}

// natGatewaysSupportedOperations lists the operation names registered by
// registerNatGatewaysOps, for GetSupportedOperations().
func natGatewaysSupportedOperations() []string {
	return []string{
		"DisassociateNatGatewayAddress",
		"AssociateNatGatewayAddress",
		"AssignPrivateNatGatewayAddress",
		"UnassignPrivateNatGatewayAddress",
	}
}

type natGatewayAddressItem struct {
	AllocationID string `xml:"allocationId"`
	PublicIP     string `xml:"publicIp"`
	PrivateIP    string `xml:"privateIp"`
}

type natGatewayAddressSet struct {
	Items []natGatewayAddressItem `xml:"item"`
}

type natGatewayItem struct {
	NatGatewayID        string               `xml:"natGatewayId"`
	SubnetID            string               `xml:"subnetId"`
	State               string               `xml:"state"`
	CreateTime          string               `xml:"createTime"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
}

type natGatewayItemSet struct {
	Items []natGatewayItem `xml:"item"`
}

type describeNatGatewaysResponse struct {
	XMLName       xml.Name          `xml:"DescribeNatGatewaysResponse"`
	Xmlns         string            `xml:"xmlns,attr"`
	RequestID     string            `xml:"requestId"`
	NatGatewaySet natGatewayItemSet `xml:"natGatewaySet"`
}

type createNatGatewayResponse struct {
	XMLName    xml.Name       `xml:"CreateNatGatewayResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	RequestID  string         `xml:"requestId"`
	NatGateway natGatewayItem `xml:"natGateway"`
}

type deleteNatGatewayResponse struct {
	XMLName      xml.Name `xml:"DeleteNatGatewayResponse"`
	Xmlns        string   `xml:"xmlns,attr"`
	RequestID    string   `xml:"requestId"`
	NatGatewayID string   `xml:"natGatewayId"`
}

func toNatGatewayItem(ngw *NatGateway) natGatewayItem {
	items := make([]natGatewayAddressItem, 0, 1+len(ngw.SecondaryPrivateIPs))
	items = append(items, natGatewayAddressItem{
		AllocationID: ngw.AllocationID,
		PublicIP:     ngw.PublicIP,
		PrivateIP:    ngw.PrivateIP,
	})

	for _, ip := range ngw.SecondaryPrivateIPs {
		items = append(items, natGatewayAddressItem{PrivateIP: ip})
	}

	return natGatewayItem{
		NatGatewayID:        ngw.ID,
		SubnetID:            ngw.SubnetID,
		State:               ngw.State,
		CreateTime:          ngw.CreateTime.Format("2006-01-02T15:04:05.000Z"),
		NatGatewayAddresses: natGatewayAddressSet{Items: items},
	}
}

func (h *Handler) handleCreateNatGateway(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	allocationID := vals.Get("AllocationId")

	if subnetID == "" || allocationID == "" {
		return nil, fmt.Errorf("%w: SubnetId and AllocationId are required", ErrInvalidParameter)
	}

	ngw, err := h.Backend.CreateNatGateway(subnetID, allocationID)
	if err != nil {
		return nil, err
	}

	return &createNatGatewayResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		NatGateway: toNatGatewayItem(ngw),
	}, nil
}

func (h *Handler) handleDeleteNatGateway(vals url.Values, reqID string) (any, error) {
	id := vals.Get("NatGatewayId")
	if id == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteNatGateway(id); err != nil {
		return nil, err
	}

	return &deleteNatGatewayResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		NatGatewayID: id,
	}, nil
}

func (h *Handler) handleDescribeNatGateways(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "NatGatewayId")
	ngws := h.Backend.DescribeNatGateways(ids)

	filters := parseEC2Filters(vals)
	ngws = applyNatGWFilters(ngws, filters, h.Backend)

	items := make([]natGatewayItem, 0, len(ngws))
	for _, ngw := range ngws {
		items = append(items, toNatGatewayItem(ngw))
	}

	return &describeNatGatewaysResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		NatGatewaySet: natGatewayItemSet{Items: items},
	}, nil
}
