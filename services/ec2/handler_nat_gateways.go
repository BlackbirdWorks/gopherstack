package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
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

	maxDrain, err := parseMaxDrainDuration(vals)
	if err != nil {
		return nil, err
	}

	ngw, err := h.Backend.DisassociateNatGatewayAddressDrain(natGatewayID, associationIDs, maxDrain)
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

// parseMaxDrainDuration reads MaxDrainDurationSeconds; absent means release immediately.
func parseMaxDrainDuration(vals url.Values) (int, error) {
	raw := vals.Get("MaxDrainDurationSeconds")
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: MaxDrainDurationSeconds must be a non-negative integer", ErrInvalidParameter)
	}

	return n, nil
}

func (h *Handler) handleAssociateNatGatewayAddress(vals url.Values, reqID string) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	allocationIDs := parseMemberList(vals, "AllocationId")

	ngw, err := h.Backend.AssociateNatGatewayAddressInZone(
		natGatewayID, vals.Get("AvailabilityZone"), vals.Get("AvailabilityZoneId"), allocationIDs)
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

func (h *Handler) handleUnassignPrivateNatGatewayAddress(
	vals url.Values,
	reqID string,
) (any, error) {
	natGatewayID := vals.Get("NatGatewayId")
	privateIPs := parseMemberList(vals, "PrivateIpAddress")

	maxDrain, err := parseMaxDrainDuration(vals)
	if err != nil {
		return nil, err
	}

	ngw, err := h.Backend.UnassignPrivateNatGatewayAddressDrain(natGatewayID, privateIPs, maxDrain)
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
	AllocationID       string `xml:"allocationId,omitempty"`
	AssociationID      string `xml:"associationId,omitempty"`
	PublicIP           string `xml:"publicIp,omitempty"`
	PrivateIP          string `xml:"privateIp,omitempty"`
	AvailabilityZone   string `xml:"availabilityZone,omitempty"`
	AvailabilityZoneID string `xml:"availabilityZoneId,omitempty"`
	Status             string `xml:"status,omitempty"`
	IsPrimary          bool   `xml:"isPrimary,omitempty"`
}

type natGatewayAddressSet struct {
	Items []natGatewayAddressItem `xml:"item"`
}

type natGatewayItem struct {
	NatGatewayID        string               `xml:"natGatewayId"`
	SubnetID            string               `xml:"subnetId,omitempty"`
	AvailabilityMode    string               `xml:"availabilityMode,omitempty"`
	RouteTableID        string               `xml:"routeTableId,omitempty"`
	AutoProvisionZones  string               `xml:"autoProvisionZones,omitempty"`
	AutoScalingIPs      string               `xml:"autoScalingIps,omitempty"`
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
	NextToken     string            `xml:"nextToken,omitempty"`
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

const natAddressStatusSucceeded = "succeeded"

func natAddressStatus(ngw *NatGateway, key, draining string) string {
	if _, ok := ngw.DrainingAddresses[key]; ok {
		return draining
	}

	return natAddressStatusSucceeded
}

func toNatGatewayItem(ngw *NatGateway, tags map[string]string) natGatewayItem {
	items := make(
		[]natGatewayAddressItem, 0,
		1+len(ngw.SecondaryAddresses)+len(ngw.SecondaryPrivateIPs),
	)
	if ngw.NatGatewayMode() == natGatewayModeZonal {
		items = append(items, natGatewayAddressItem{
			AllocationID:       ngw.AllocationID,
			AssociationID:      ngw.AssociationID,
			PublicIP:           ngw.PublicIP,
			PrivateIP:          ngw.PrivateIP,
			AvailabilityZone:   ngw.AvailabilityZone,
			AvailabilityZoneID: availabilityZoneID(ngw.AvailabilityZone),
			Status:             natAddressStatusSucceeded,
			IsPrimary:          true,
		})
	}

	for _, za := range ngw.ZoneAddresses {
		items = append(items, natGatewayAddressItem{
			AllocationID:       za.AllocationID,
			AssociationID:      za.AssociationID,
			PublicIP:           za.PublicIP,
			PrivateIP:          za.PrivateIP,
			AvailabilityZone:   za.AvailabilityZone,
			AvailabilityZoneID: availabilityZoneID(za.AvailabilityZone),
			Status:             natAddressStatus(ngw, za.AssociationID, "disassociating"),
		})
	}

	for _, sa := range ngw.SecondaryAddresses {
		items = append(items, natGatewayAddressItem{
			AllocationID:     sa.AllocationID,
			AssociationID:    sa.AssociationID,
			PublicIP:         sa.PublicIP,
			PrivateIP:        sa.PrivateIP,
			AvailabilityZone: ngw.AvailabilityZone,
			Status:           natAddressStatus(ngw, sa.AssociationID, "disassociating"),
		})
	}

	for _, ip := range ngw.SecondaryPrivateIPs {
		items = append(items, natGatewayAddressItem{
			PrivateIP:        ip,
			AvailabilityZone: ngw.AvailabilityZone,
			Status:           natAddressStatus(ngw, ip, "unassigning"),
		})
	}

	return natGatewayItem{
		NatGatewayID:        ngw.ID,
		SubnetID:            ngw.SubnetID,
		AvailabilityMode:    ngw.NatGatewayMode(),
		RouteTableID:        ngw.RouteTableID,
		AutoProvisionZones:  ngw.AutoProvisionZones,
		AutoScalingIPs:      ngw.AutoScalingIPs,
		VpcID:               ngw.VPCID,
		State:               ngw.State,
		ConnectivityType:    ngw.ConnectivityType,
		CreateTime:          ngw.CreateTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		NatGatewayAddresses: natGatewayAddressSet{Items: items},
		TagSet:              tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateNatGateway(vals url.Values, reqID string) (any, error) {
	if vals.Get("AvailabilityMode") != "" {
		return h.handleCreateAvailabilityModeNatGateway(vals, reqID)
	}

	if vals.Get("VpcId") != "" || vals.Get("AvailabilityZoneAddress.1.AvailabilityZone") != "" {
		return nil, fmt.Errorf("%w: VpcId and AvailabilityZoneAddress apply to regional NAT gateways only",
			ErrInvalidParameter)
	}

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

func (h *Handler) handleCreateAvailabilityModeNatGateway(vals url.Values, reqID string) (any, error) {
	mode := vals.Get("AvailabilityMode")

	switch mode {
	case natGatewayModeZonal:
		if vals.Get("VpcId") != "" || vals.Get("AvailabilityZoneAddress.1.AvailabilityZone") != "" {
			return nil, fmt.Errorf("%w: VpcId and AvailabilityZoneAddress apply to regional NAT gateways only",
				ErrInvalidParameter)
		}

		vals.Del("AvailabilityMode")

		return h.handleCreateNatGateway(vals, reqID)
	case natGatewayModeRegional:
	default:
		return nil, fmt.Errorf("%w: invalid AvailabilityMode %q", ErrInvalidParameter, mode)
	}

	if vals.Get("SubnetId") != "" || vals.Get("AllocationId") != "" {
		return nil, fmt.Errorf("%w: SubnetId and AllocationId cannot be used with a regional NAT gateway",
			ErrInvalidParameter)
	}

	if ct := vals.Get("ConnectivityType"); ct != "" && ct != natGatewayConnectivityTypePublic {
		return nil, fmt.Errorf("%w: a regional NAT gateway supports public connectivity only", ErrInvalidParameter)
	}

	zones := parseNatGatewayZoneRequests(vals)

	ngw, err := h.Backend.CreateRegionalNatGateway(vals.Get("VpcId"), zones, parseTagSpecification(vals, "natgateway"))
	if err != nil {
		return nil, err
	}

	return &createNatGatewayResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		NatGateway: toNatGatewayItem(ngw, h.Backend.TagsForResource(ngw.ID)),
	}, nil
}

func parseNatGatewayZoneRequests(vals url.Values) []NatGatewayZoneRequest {
	var zones []NatGatewayZoneRequest

	for i := 1; ; i++ {
		prefix := fmt.Sprintf("AvailabilityZoneAddress.%d.", i)
		z := NatGatewayZoneRequest{
			AvailabilityZone:   vals.Get(prefix + "AvailabilityZone"),
			AvailabilityZoneID: vals.Get(prefix + "AvailabilityZoneId"),
			AllocationIDs:      parseMemberList(vals, prefix+"AllocationId"),
		}

		if z.AvailabilityZone == "" && z.AvailabilityZoneID == "" && len(z.AllocationIDs) == 0 {
			return zones
		}

		zones = append(zones, z)
	}
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

	if err := requireAllIDsPresent(
		ids, ngws, func(n *NatGateway) string { return n.ID }, ErrNatGatewayNotFound,
	); err != nil {
		return nil, err
	}

	filters := parseEC2Filters(vals)
	ngws = applyNatGWFilters(ngws, filters, h.Backend)

	items := make([]natGatewayItem, 0, len(ngws))
	for _, ngw := range ngws {
		items = append(items, toNatGatewayItem(ngw, h.Backend.TagsForResource(ngw.ID)))
	}

	return finishPaged(vals, &describeNatGatewaysResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		NatGatewaySet: natGatewayItemSet{Items: items},
	})
}
