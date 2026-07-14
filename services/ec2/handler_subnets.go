package ec2

import (
	"encoding/xml"
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

type sgVpcAssocStateItem struct {
	State string `xml:"state"`
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
	resp.Subnet.State = "available"

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

type instanceImageMetadataItem struct {
	InstanceID string `xml:"instanceId"`
	ImageID    string `xml:"imageId"`
	ImageState string `xml:"imageState"`
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
	if err := h.Backend.DeleteSubnetCidrReservation(reservationID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DeleteSubnetCidrReservationResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type getSubnetCidrReservationsResponse struct {
	XMLName                    xml.Name `xml:"GetSubnetCidrReservationsResponse"`
	RequestID                  string   `xml:"requestId"`
	SubnetIpv4CidrReservations struct {
		Items []subnetCidrReservationItem2 `xml:"item"`
	} `xml:"subnetIpv4CidrReservations"`
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
		resp.SubnetIpv4CidrReservations.Items = append(
			resp.SubnetIpv4CidrReservations.Items,
			subnetCidrReservationItem2{
				SubnetCidrReservationID: r.SubnetCIDRReservationID,
				SubnetID:                r.SubnetID,
				Cidr:                    r.CIDR,
				ReservationType:         r.ReservationType,
				State:                   r.State,
			},
		)
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
