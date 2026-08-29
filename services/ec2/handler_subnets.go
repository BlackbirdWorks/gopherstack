package ec2

import (
	"encoding/xml"
	"fmt"
	"net"
	"net/url"
	"strconv"
)

type createDefaultSubnetResponse struct {
	XMLName   xml.Name `xml:"CreateDefaultSubnetResponse"`
	RequestID string   `xml:"requestId"`
	Subnet    struct {
		SubnetID         string `xml:"subnetId"`
		VpcID            string `xml:"vpcId"`
		CIDRBlock        string `xml:"cidrBlock"`
		AvailabilityZone string `xml:"availabilityZone"`
		State            string `xml:"state"`
		IsDefault        bool   `xml:"defaultForAz"`
	} `xml:"subnet"`
}

type subnetCIDRAssocResponse struct {
	XMLName                  xml.Name `xml:"AssociateSubnetCidrBlockResponse"`
	RequestID                string   `xml:"requestId"`
	SubnetID                 string   `xml:"subnetId"`
	Ipv6CidrBlockAssociation struct {
		AssociationID      string `xml:"associationId"`
		Ipv6CIDRBlock      string `xml:"ipv6CidrBlock"`
		Ipv6CidrBlockState struct {
			State string `xml:"state"`
		} `xml:"ipv6CidrBlockState"`
	} `xml:"ipv6CidrBlockAssociation"`
}

type disassociateSubnetCIDRResponse struct {
	XMLName   xml.Name `xml:"DisassociateSubnetCidrBlockResponse"`
	RequestID string   `xml:"requestId"`
	SubnetID  string   `xml:"subnetId"`
}

func (h *Handler) handleCreateDefaultSubnet(vals url.Values, reqID string) (any, error) {
	az := vals.Get("AvailabilityZone")
	subnet, err := h.Backend.CreateDefaultSubnet(az)
	if err != nil {
		return nil, err
	}
	resp := &createDefaultSubnetResponse{RequestID: reqID}
	resp.Subnet.SubnetID = subnet.ID
	resp.Subnet.VpcID = subnet.VPCID
	resp.Subnet.CIDRBlock = subnet.CIDRBlock
	resp.Subnet.AvailabilityZone = subnet.AvailabilityZone
	resp.Subnet.IsDefault = subnet.IsDefault
	resp.Subnet.State = stateAvailableImg

	return resp, nil
}

func (h *Handler) handleAssociateSubnetCidrBlock(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	ipv6CIDR := vals.Get("Ipv6CidrBlock")

	assoc, err := h.Backend.AssociateSubnetCidrBlock(subnetID, ipv6CIDR)
	if err != nil {
		return nil, err
	}
	resp := &subnetCIDRAssocResponse{RequestID: reqID, SubnetID: subnetID}
	resp.Ipv6CidrBlockAssociation.AssociationID = assoc.AssociationID
	resp.Ipv6CidrBlockAssociation.Ipv6CIDRBlock = assoc.IPv6CIDRBlock
	resp.Ipv6CidrBlockAssociation.Ipv6CidrBlockState.State = assoc.State

	return resp, nil
}

func (h *Handler) handleDisassociateSubnetCidrBlock(vals url.Values, reqID string) (any, error) {
	assocID := vals.Get("AssociationId")
	subnetID, err := h.Backend.DisassociateSubnetCidrBlock(assocID)
	if err != nil {
		return nil, err
	}

	return &disassociateSubnetCIDRResponse{RequestID: reqID, SubnetID: subnetID}, nil
}

type createSubnetCidrReservationResponse struct {
	XMLName               xml.Name                  `xml:"CreateSubnetCidrReservationResponse"`
	RequestID             string                    `xml:"requestId"`
	SubnetCidrReservation subnetCidrReservationItem `xml:"subnetCidrReservation"`
}

// imageMetadataItem matches types.ImageMetadata, nested under
// InstanceImageMetadata's "imageMetadata" element (ec2@v1.319.1
// deserializers.go:107294) -- imageId/imageState do NOT sit at the top
// level of instanceImageMetadataItem.
type imageMetadataItem struct {
	ImageID    string `xml:"imageId,omitempty"`
	ImageState string `xml:"imageState,omitempty"`
}

// instanceImageMetadataItem matches types.InstanceImageMetadata
// (ec2@v1.319.1 deserializers.go:112881). Operator and Tags are documented
// gaps: this backend tracks no org-managed-resource or per-instance tag
// state for this report.
type instanceImageMetadataItem struct {
	ImageMetadata    imageMetadataItem `xml:"imageMetadata"`
	InstanceID       string            `xml:"instanceId,omitempty"`
	AvailabilityZone string            `xml:"availabilityZone,omitempty"`
	InstanceType     string            `xml:"instanceType,omitempty"`
	LaunchTime       string            `xml:"launchTime,omitempty"`
	InstanceOwnerID  string            `xml:"instanceOwnerId,omitempty"`
	InstanceState    stateItem         `xml:"instanceState"`
}

func toInstanceImageMetadataItem(item InstanceImageMetadataItem) instanceImageMetadataItem {
	wire := instanceImageMetadataItem{
		InstanceID:       item.InstanceID,
		AvailabilityZone: item.AvailabilityZone,
		InstanceType:     item.InstanceType,
		InstanceOwnerID:  item.OwnerID,
		InstanceState:    stateItem{Name: item.StateName, Code: item.StateCode},
		ImageMetadata:    imageMetadataItem{ImageID: item.ImageID, ImageState: item.ImageState},
	}
	if !item.LaunchTime.IsZero() {
		wire.LaunchTime = item.LaunchTime.UTC().Format(timeLayoutISO)
	}

	return wire
}

func (h *Handler) handleCreateSubnetCidrReservation(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	cidr := vals.Get("Cidr")
	reservationType := vals.Get("ReservationType")
	if reservationType == "" {
		reservationType = "prefix"
	}
	description := vals.Get("Description")

	reservation, err := h.Backend.CreateSubnetCidrReservation(
		subnetID,
		cidr,
		reservationType,
		description,
	)
	if err != nil {
		return nil, err
	}

	return &createSubnetCidrReservationResponse{
		RequestID: reqID,
		SubnetCidrReservation: subnetCidrReservationItem{
			SubnetCidrReservationID: reservation.SubnetCIDRReservationID,
			SubnetID:                reservation.SubnetID,
			Cidr:                    reservation.CIDR,
			ReservationType:         reservation.ReservationType,
			Description:             reservation.Description,
			OwnerID:                 reservation.OwnerID,
			State:                   reservation.State,
		},
	}, nil
}

func (h *Handler) handleDeleteSubnetCidrReservation(vals url.Values, reqID string) (any, error) {
	reservationID := vals.Get("SubnetCidrReservationId")

	reservation, err := h.Backend.DeleteSubnetCidrReservation(reservationID)
	if err != nil {
		return nil, err
	}

	return &deleteSubnetCidrReservationResponse{
		RequestID: reqID,
		DeletedSubnetCidrReservation: subnetCidrReservationItem{
			SubnetCidrReservationID: reservation.SubnetCIDRReservationID,
			SubnetID:                reservation.SubnetID,
			Cidr:                    reservation.CIDR,
			ReservationType:         reservation.ReservationType,
			Description:             reservation.Description,
			OwnerID:                 reservation.OwnerID,
			State:                   reservation.State,
		},
	}, nil
}

type deleteSubnetCidrReservationResponse struct {
	XMLName                      xml.Name                  `xml:"DeleteSubnetCidrReservationResponse"`
	RequestID                    string                    `xml:"requestId"`
	DeletedSubnetCidrReservation subnetCidrReservationItem `xml:"deletedSubnetCidrReservation"`
}

type getSubnetCidrReservationsResponse struct {
	XMLName                    xml.Name `xml:"GetSubnetCidrReservationsResponse"`
	RequestID                  string   `xml:"requestId"`
	SubnetIpv4CidrReservations struct {
		Items []subnetCidrReservationItem `xml:"item"`
	} `xml:"subnetIpv4CidrReservationSet"`
	SubnetIpv6CidrReservations struct {
		Items []subnetCidrReservationItem `xml:"item"`
	} `xml:"subnetIpv6CidrReservationSet"`
}

type sgForVpcItem struct {
	GroupID     string `xml:"groupId"`
	GroupName   string `xml:"groupName"`
	Description string `xml:"description"`
}

func (h *Handler) handleGetSubnetCidrReservations(vals url.Values, reqID string) (any, error) {
	subnetID := vals.Get("SubnetId")
	reservations, err := h.Backend.GetSubnetCidrReservations(subnetID)
	if err != nil {
		return nil, err
	}

	resp := &getSubnetCidrReservationsResponse{RequestID: reqID}
	for _, r := range reservations {
		item := subnetCidrReservationItem{
			SubnetCidrReservationID: r.SubnetCIDRReservationID,
			SubnetID:                r.SubnetID,
			Cidr:                    r.CIDR,
			ReservationType:         r.ReservationType,
			Description:             r.Description,
			OwnerID:                 r.OwnerID,
			State:                   r.State,
		}

		ip, _, parseErr := net.ParseCIDR(r.CIDR)
		if parseErr == nil && ip.To4() == nil {
			resp.SubnetIpv6CidrReservations.Items = append(resp.SubnetIpv6CidrReservations.Items, item)
		} else {
			resp.SubnetIpv4CidrReservations.Items = append(resp.SubnetIpv4CidrReservations.Items, item)
		}
	}

	return resp, nil
}

func (h *Handler) handleModifySubnetAttribute(vals url.Values, reqID string) (any, error) {
	var attr string
	var valueStr string

	switch {
	case vals.Get("MapPublicIpOnLaunch.Value") != "":
		attr = attrMapPublicIPOnLaunch
		valueStr = vals.Get("MapPublicIpOnLaunch.Value")
	default:
		attr = attrMapPublicIPOnLaunch
		valueStr = ec2BooleanTrue
	}

	value, _ := strconv.ParseBool(valueStr)

	if err := h.Backend.ModifySubnetAttribute(vals.Get("SubnetId"), attr, value); err != nil {
		return nil, err
	}

	return &genericReturnResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

// ---- NACL handlers ----

// registerSubnetsOps registers the Subnets operation handlers.
func registerSubnetsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateDefaultSubnet"] = h.handleCreateDefaultSubnet
	ops["AssociateSubnetCidrBlock"] = h.handleAssociateSubnetCidrBlock
	ops["DisassociateSubnetCidrBlock"] = h.handleDisassociateSubnetCidrBlock
	ops["CreateSubnetCidrReservation"] = h.handleCreateSubnetCidrReservation
	ops["DeleteSubnetCidrReservation"] = h.handleDeleteSubnetCidrReservation
	ops["GetSubnetCidrReservations"] = h.handleGetSubnetCidrReservations
	ops["ModifySubnetAttribute"] = h.handleModifySubnetAttribute
}

// subnetsSupportedOperations lists the operation names registered by
// registerSubnetsOps, for GetSupportedOperations().
func subnetsSupportedOperations() []string {
	return []string{
		"CreateDefaultSubnet",
		"AssociateSubnetCidrBlock",
		"DisassociateSubnetCidrBlock",
		"CreateSubnetCidrReservation",
		"DeleteSubnetCidrReservation",
		"GetSubnetCidrReservations",
		"ModifySubnetAttribute",
	}
}

func (h *Handler) handleDescribeSubnets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SubnetId")
	subnets := h.Backend.DescribeSubnets(ids)

	filters := parseEC2Filters(vals)
	subnets = applySubnetFilters(subnets, filters, h.Backend)

	items := make([]subnetItem, 0, len(subnets))
	for _, s := range subnets {
		items = append(items, toSubnetItem(s, h.Backend.TagsForResource(s.ID)))
	}

	return &describeSubnetsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		SubnetSet: subnetItemSet{Items: items},
	}, nil
}

func (h *Handler) handleCreateSubnet(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	cidr := vals.Get("CidrBlock")
	az := vals.Get("AvailabilityZone")
	outpostArn := vals.Get("OutpostArn")

	s, err := h.Backend.CreateSubnetWithOutpost(vpcID, cidr, az, outpostArn)
	if err != nil {
		return nil, err
	}

	if tags := parseTagSpecification(vals, "subnet"); len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{s.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createSubnetResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Subnet:    toSubnetItem(s, h.Backend.TagsForResource(s.ID)),
	}, nil
}

func (h *Handler) handleDeleteSubnet(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SubnetId")
	if id == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteSubnet(id); err != nil {
		return nil, err
	}

	return &deleteSubnetResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func toSubnetItem(s *Subnet, tags map[string]string) subnetItem {
	return subnetItem{
		SubnetID:         s.ID,
		VPCID:            s.VPCID,
		CIDRBlock:        s.CIDRBlock,
		AvailabilityZone: s.AvailabilityZone,
		OutpostArn:       s.OutpostArn,
		State:            stateAvailable,
		TagSet:           tagItemsFromMap(tags),
	}
}

type subnetItem struct {
	SubnetID         string          `xml:"subnetId"`
	VPCID            string          `xml:"vpcId"`
	CIDRBlock        string          `xml:"cidrBlock"`
	AvailabilityZone string          `xml:"availabilityZone"`
	OutpostArn       string          `xml:"outpostArn,omitempty"`
	State            string          `xml:"state"`
	TagSet           []simpleTagItem `xml:"tagSet>item"`
}

type subnetItemSet struct {
	Items []subnetItem `xml:"item"`
}

type describeSubnetsResponse struct {
	XMLName   xml.Name      `xml:"DescribeSubnetsResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	SubnetSet subnetItemSet `xml:"subnetSet"`
}

type createSubnetResponse struct {
	XMLName   xml.Name   `xml:"CreateSubnetResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	Subnet    subnetItem `xml:"subnet"`
}

type deleteSubnetResponse struct {
	XMLName   xml.Name `xml:"DeleteSubnetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}
