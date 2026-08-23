package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

type associateNatGatewayAddressResponse struct {
	XMLName             xml.Name             `xml:"AssociateNatGatewayAddressResponse"`
	Xmlns               string               `xml:"xmlns,attr"`
	RequestID           string               `xml:"requestId"`
	NatGatewayID        string               `xml:"natGatewayId,omitempty"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
}

type disassociateNatGatewayAddressResponse struct {
	XMLName             xml.Name             `xml:"DisassociateNatGatewayAddressResponse"`
	Xmlns               string               `xml:"xmlns,attr"`
	RequestID           string               `xml:"requestId"`
	NatGatewayID        string               `xml:"natGatewayId,omitempty"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
}

func (h *Handler) handleDisassociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	associationIDs := parseMemberList(vals, "AssociationId")

	ngw, err := h.Backend.DisassociateNatGatewayAddress(natGatewayID, associationIDs)
	if err != nil {
		return nil, err
	}

	item := toNatGatewayItem(ngw, nil)

	return &disassociateNatGatewayAddressResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NatGatewayID:        ngw.ID,
		NatGatewayAddresses: item.NatGatewayAddresses,
	}, nil
}

func (h *Handler) handleAssociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	allocationIDs := parseMemberList(vals, "AllocationId")

	ngw, err := h.Backend.AssociateNatGatewayAddress(natGatewayID, allocationIDs)
	if err != nil {
		return nil, err
	}

	item := toNatGatewayItem(ngw, nil)

	return &associateNatGatewayAddressResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NatGatewayID:        ngw.ID,
		NatGatewayAddresses: item.NatGatewayAddresses,
	}, nil
}

func (h *Handler) handleAssignPrivateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")

	count := 0
	if v := vals.Get("PrivateIpAddressCount"); v != "" {
		_, _ = fmt.Sscan(v, &count)
	}

	ips := parseMemberList(vals, "PrivateIpAddress")

	ngw, err := h.Backend.AssignPrivateNatGatewayAddress(natGatewayID, count, ips)
	if err != nil {
		return nil, err
	}

	item := toNatGatewayItem(ngw, nil)

	return &assignPrivateNatGatewayAddressResponse{
		Xmlns:               ec2XMLNS,
		RequestID:           reqID,
		NatGatewayID:        ngw.ID,
		NatGatewayAddresses: item.NatGatewayAddresses,
	}, nil
}

// assignPrivateNatGatewayAddressResponse matches
// AssignPrivateNatGatewayAddressOutput (ec2@v1.319.1
// api_op_AssignPrivateNatGatewayAddress.go): natGatewayAddressSet and
// natGatewayId, no Return member -- the same shape as the sibling
// Associate/Disassociate/UnassignPrivateNatGatewayAddress ops.
type assignPrivateNatGatewayAddressResponse struct {
	XMLName             xml.Name             `xml:"AssignPrivateNatGatewayAddressResponse"`
	Xmlns               string               `xml:"xmlns,attr"`
	RequestID           string               `xml:"requestId"`
	NatGatewayID        string               `xml:"natGatewayId,omitempty"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
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

	item := toNatGatewayItem(ngw, nil)

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
	AllocationID     string `xml:"allocationId,omitempty"`
	AssociationID    string `xml:"associationId,omitempty"`
	PublicIP         string `xml:"publicIp,omitempty"`
	PrivateIP        string `xml:"privateIp,omitempty"`
	AvailabilityZone string `xml:"availabilityZone,omitempty"`
	IsPrimary        bool   `xml:"isPrimary,omitempty"`
}

type natGatewayAddressSet struct {
	Items []natGatewayAddressItem `xml:"item"`
}

type natGatewayItem struct {
	NatGatewayID        string               `xml:"natGatewayId"`
	SubnetID            string               `xml:"subnetId"`
	VpcID               string               `xml:"vpcId,omitempty"`
	State               string               `xml:"state"`
	ConnectivityType    string               `xml:"connectivityType,omitempty"`
	CreateTime          string               `xml:"createTime"`
	NatGatewayAddresses natGatewayAddressSet `xml:"natGatewayAddressSet"`
	TagSet              []simpleTagItem      `xml:"tagSet>item"`
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

func toNatGatewayItem(ngw *NatGateway, tags map[string]string) natGatewayItem {
	items := make(
		[]natGatewayAddressItem, 0,
		1+len(ngw.SecondaryAddresses)+len(ngw.SecondaryPrivateIPs),
	)
	items = append(items, natGatewayAddressItem{
		AllocationID:     ngw.AllocationID,
		AssociationID:    ngw.AssociationID,
		PublicIP:         ngw.PublicIP,
		PrivateIP:        ngw.PrivateIP,
		AvailabilityZone: ngw.AvailabilityZone,
		IsPrimary:        true,
	})

	for _, sa := range ngw.SecondaryAddresses {
		items = append(items, natGatewayAddressItem{
			AllocationID:     sa.AllocationID,
			AssociationID:    sa.AssociationID,
			PublicIP:         sa.PublicIP,
			PrivateIP:        sa.PrivateIP,
			AvailabilityZone: ngw.AvailabilityZone,
		})
	}

	for _, ip := range ngw.SecondaryPrivateIPs {
		items = append(items, natGatewayAddressItem{PrivateIP: ip, AvailabilityZone: ngw.AvailabilityZone})
	}

	return natGatewayItem{
		NatGatewayID:        ngw.ID,
		SubnetID:            ngw.SubnetID,
		VpcID:               ngw.VPCID,
		State:               ngw.State,
		ConnectivityType:    ngw.ConnectivityType,
		CreateTime:          ngw.CreateTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		NatGatewayAddresses: natGatewayAddressSet{Items: items},
		TagSet:              tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateNatGateway(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	allocationID := vals.Get("AllocationId")

	if subnetID == "" || allocationID == "" {
		return nil, fmt.Errorf("%w: SubnetId and AllocationId are required", ErrInvalidParameter)
	}

	tags := parseTagSpecification(vals, "natgateway")

	ngw, err := h.Backend.CreateNatGateway(subnetID, allocationID, tags)
	if err != nil {
		return nil, err
	}

	return &createNatGatewayResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		NatGateway: toNatGatewayItem(ngw, h.Backend.TagsForResource(ngw.ID)),
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
		items = append(items, toNatGatewayItem(ngw, h.Backend.TagsForResource(ngw.ID)))
	}

	return &describeNatGatewaysResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		NatGatewaySet: natGatewayItemSet{Items: items},
	}, nil
}
