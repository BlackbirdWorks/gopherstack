package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- XML response types for accept/advertise/allocate operations ----

type addressTransferItem struct {
	AllocationID        string `xml:"allocationId"`
	TransferAccountID   string `xml:"transferAccountId,omitempty"`
	TransferOfferStatus string `xml:"transferOfferStatus"`
}

type acceptAddressTransferResponse struct {
	XMLName         xml.Name            `xml:"AcceptAddressTransferResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	RequestID       string              `xml:"requestId"`
	AddressTransfer addressTransferItem `xml:"addressTransfer"`
}

type capacityReservationItem struct {
	CapacityReservationID  string `xml:"capacityReservationId"`
	InstanceType           string `xml:"instanceType"`
	AvailabilityZone       string `xml:"availabilityZone"`
	OwnedBy                string `xml:"ownedBy,omitempty"`
	State                  string `xml:"state"`
	AvailableInstanceCount int    `xml:"availableInstanceCount"`
	TotalInstanceCount     int    `xml:"totalInstanceCount"`
}

type acceptCapacityReservationBillingOwnershipResponse struct {
	XMLName             xml.Name                `xml:"AcceptCapacityReservationBillingOwnershipResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	RequestID           string                  `xml:"requestId"`
	CapacityReservation capacityReservationItem `xml:"capacityReservation"`
}

type acceptReservedInstancesExchangeQuoteResponse struct {
	XMLName    xml.Name `xml:"AcceptReservedInstancesExchangeQuoteResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	ExchangeID string   `xml:"exchangeId"`
}

type tgwMulticastDomainAssociationItem struct {
	TransitGatewayMulticastDomainID string `xml:"transitGatewayMulticastDomainId"`
	TransitGatewayAttachmentID      string `xml:"transitGatewayAttachmentId"`
	SubnetID                        string `xml:"subnetId"`
	State                           string `xml:"state"`
}

type tgwMulticastDomainAssociationSet struct {
	Items []tgwMulticastDomainAssociationItem `xml:"item"`
}

type acceptTransitGatewayMulticastDomainAssociationsResponse struct {
	XMLName      xml.Name                         `xml:"AcceptTransitGatewayMulticastDomainAssociationsResponse"`
	Xmlns        string                           `xml:"xmlns,attr"`
	RequestID    string                           `xml:"requestId"`
	Associations tgwMulticastDomainAssociationSet `xml:"associations"`
}

type peeringTgwInfoItem struct {
	TransitGatewayID string `xml:"transitGatewayId,omitempty"`
}

// tgwPeeringAttachmentItem mirrors the real TransitGatewayPeeringAttachment
// wire shape (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentTransitGatewayPeeringAttachment): the
// requester/accepter transit gateway IDs nest under requesterTgwInfo/
// accepterTgwInfo, not flat top-level fields.
type tgwPeeringAttachmentItem struct {
	TransitGatewayAttachmentID string             `xml:"transitGatewayAttachmentId"`
	RequesterTgwInfo           peeringTgwInfoItem `xml:"requesterTgwInfo"`
	AccepterTgwInfo            peeringTgwInfoItem `xml:"accepterTgwInfo"`
	State                      string             `xml:"state"`
	CreationTime               string             `xml:"creationTime,omitempty"`
	TagSet                     []simpleTagItem    `xml:"tagSet>item"`
}

type acceptTransitGatewayPeeringAttachmentResponse struct {
	XMLName                  xml.Name                 `xml:"AcceptTransitGatewayPeeringAttachmentResponse"`
	Xmlns                    string                   `xml:"xmlns,attr"`
	RequestID                string                   `xml:"requestId"`
	TransitGatewayPeeringAtt tgwPeeringAttachmentItem `xml:"transitGatewayPeeringAttachment"`
}

type tgwVpcAttachmentItem struct {
	TransitGatewayAttachmentID string          `xml:"transitGatewayAttachmentId"`
	TransitGatewayID           string          `xml:"transitGatewayId,omitempty"`
	VpcID                      string          `xml:"vpcId,omitempty"`
	State                      string          `xml:"state"`
	CreationTime               string          `xml:"creationTime,omitempty"`
	SubnetIDs                  []string        `xml:"subnetIds>item,omitempty"`
	TagSet                     []simpleTagItem `xml:"tagSet>item"`
}

type acceptTransitGatewayVpcAttachmentResponse struct {
	XMLName              xml.Name             `xml:"AcceptTransitGatewayVpcAttachmentResponse"`
	Xmlns                string               `xml:"xmlns,attr"`
	RequestID            string               `xml:"requestId"`
	TransitGatewayVpcAtt tgwVpcAttachmentItem `xml:"transitGatewayVpcAttachment"`
}

type vpcEndpointConnectionItem struct {
	ServiceID     string `xml:"serviceId"`
	VpcEndpointID string `xml:"vpcEndpointId"`
	State         string `xml:"vpcEndpointState"`
}

type vpcEndpointConnectionSet struct {
	Items []vpcEndpointConnectionItem `xml:"item"`
}

type acceptVpcEndpointConnectionsResponse struct {
	XMLName xml.Name `xml:"AcceptVpcEndpointConnectionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	// RequestID is the AWS request identifier.
	RequestID string `xml:"requestId"`
	// Unsuccessful holds items that FAILED to be accepted.
	// On full success this list is empty; the XML element name matches the AWS API.
	Unsuccessful vpcEndpointConnectionSet `xml:"unsuccessful"`
}

type vpcPeeringConnectionVpcInfoItem struct {
	VpcID string `xml:"vpcId,omitempty"`
}

type vpcPeeringConnectionStatusItem struct {
	Code string `xml:"code,omitempty"`
}

// vpcPeeringConnectionItem mirrors the real VpcPeeringConnection wire shape
// (ec2@v1.319.1 deserializers.go, awsEc2query_deserializeDocumentVpcPeeringConnection):
// the VPC IDs nest under requesterVpcInfo/accepterVpcInfo, and status is a
// {code, message} object, not flat top-level fields.
type vpcPeeringConnectionItem struct {
	VpcPeeringConnectionID string                          `xml:"vpcPeeringConnectionId"`
	RequesterVpcInfo       vpcPeeringConnectionVpcInfoItem `xml:"requesterVpcInfo"`
	AccepterVpcInfo        vpcPeeringConnectionVpcInfoItem `xml:"accepterVpcInfo"`
	Status                 vpcPeeringConnectionStatusItem  `xml:"status"`
}

func toVpcPeeringConnectionItem(pc *VpcPeeringConnection) vpcPeeringConnectionItem {
	return vpcPeeringConnectionItem{
		VpcPeeringConnectionID: pc.VpcPeeringConnectionID,
		RequesterVpcInfo:       vpcPeeringConnectionVpcInfoItem{VpcID: pc.RequesterVpcID},
		AccepterVpcInfo:        vpcPeeringConnectionVpcInfoItem{VpcID: pc.AccepterVpcID},
		Status:                 vpcPeeringConnectionStatusItem{Code: pc.State},
	}
}

type acceptVpcPeeringConnectionResponse struct {
	XMLName              xml.Name                 `xml:"AcceptVpcPeeringConnectionResponse"`
	Xmlns                string                   `xml:"xmlns,attr"`
	RequestID            string                   `xml:"requestId"`
	VpcPeeringConnection vpcPeeringConnectionItem `xml:"vpcPeeringConnection"`
}

type byoipCidrItem struct {
	Cidr          string `xml:"cidr"`
	State         string `xml:"state"`
	StatusMessage string `xml:"statusMessage,omitempty"`
}

type advertiseByoipCidrResponse struct {
	XMLName   xml.Name      `xml:"AdvertiseByoipCidrResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	ByoipCidr byoipCidrItem `xml:"byoipCidr"`
}

type hostIDSet struct {
	Items []string `xml:"item"`
}

type allocateHostsResponse struct {
	XMLName   xml.Name  `xml:"AllocateHostsResponse"`
	Xmlns     string    `xml:"xmlns,attr"`
	RequestID string    `xml:"requestId"`
	HostIDs   hostIDSet `xml:"hostIdSet"`
}

// ---- handler functions ----

func (h *Handler) handleAcceptAddressTransfer(vals url.Values, reqID string) (any, error) {
	address := vals.Get("Address")
	if address == "" {
		return nil, fmt.Errorf("%w: Address is required", ErrInvalidParameter)
	}

	transfer, err := h.Backend.AcceptAddressTransfer(address)
	if err != nil {
		return nil, err
	}

	return &acceptAddressTransferResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		AddressTransfer: addressTransferItem{
			AllocationID:        transfer.AllocationID,
			TransferAccountID:   transfer.TransferAccountID,
			TransferOfferStatus: transfer.TransferOfferStatus,
		},
	}, nil
}

func (h *Handler) handleAcceptCapacityReservationBillingOwnership(
	vals url.Values,
	reqID string,
) (any, error) {
	capacityReservationID := vals.Get("CapacityReservationId")
	if capacityReservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	cr, err := h.Backend.AcceptCapacityReservationBillingOwnership(capacityReservationID)
	if err != nil {
		return nil, err
	}

	return &acceptCapacityReservationBillingOwnershipResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		CapacityReservation: capacityReservationItem{
			CapacityReservationID:  cr.CapacityReservationID,
			InstanceType:           cr.InstanceType,
			AvailabilityZone:       cr.AvailabilityZone,
			AvailableInstanceCount: cr.AvailableInstanceCount,
			TotalInstanceCount:     cr.TotalInstanceCount,
			OwnedBy:                cr.OwnedBy,
			State:                  cr.State,
		},
	}, nil
}

func (h *Handler) handleAcceptReservedInstancesExchangeQuote(
	vals url.Values,
	reqID string,
) (any, error) {
	var ids []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("ReservedInstanceId.%d", i))
		if id == "" {
			break
		}

		ids = append(ids, id)
	}

	if len(ids) == 0 {
		return nil, fmt.Errorf(
			"%w: at least one ReservedInstanceId is required",
			ErrInvalidParameter,
		)
	}

	exchange, err := h.Backend.AcceptReservedInstancesExchangeQuote(ids)
	if err != nil {
		return nil, err
	}

	return &acceptReservedInstancesExchangeQuoteResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		ExchangeID: exchange.ExchangeID,
	}, nil
}

func (h *Handler) handleAcceptTransitGatewayMulticastDomainAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	domainID := vals.Get("TransitGatewayMulticastDomainId")
	attachmentID := vals.Get("TransitGatewayAttachmentId")

	var subnetIDs []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("SubnetIds.%d", i))
		if id == "" {
			break
		}

		subnetIDs = append(subnetIDs, id)
	}

	assocs, err := h.Backend.AcceptTransitGatewayMulticastDomainAssociations(
		domainID,
		attachmentID,
		subnetIDs,
	)
	if err != nil {
		return nil, err
	}

	resp := &acceptTransitGatewayMulticastDomainAssociationsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}

	for _, a := range assocs {
		resp.Associations.Items = append(resp.Associations.Items, tgwMulticastDomainAssociationItem{
			TransitGatewayMulticastDomainID: a.TransitGatewayMulticastDomainID,
			TransitGatewayAttachmentID:      a.TransitGatewayAttachmentID,
			SubnetID:                        a.SubnetID,
			State:                           a.State,
		})
	}

	return resp, nil
}

func (h *Handler) handleAcceptTransitGatewayPeeringAttachment(
	vals url.Values,
	reqID string,
) (any, error) {
	attachmentID := vals.Get("TransitGatewayAttachmentId")
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	att, err := h.Backend.AcceptTransitGatewayPeeringAttachment(attachmentID)
	if err != nil {
		return nil, err
	}

	return &acceptTransitGatewayPeeringAttachmentResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		TransitGatewayPeeringAtt: toTGWPeeringAttachmentItem(
			att, h.Backend.TagsForResource(att.TransitGatewayAttachmentID),
		),
	}, nil
}

func (h *Handler) handleAcceptTransitGatewayVpcAttachment(
	vals url.Values,
	reqID string,
) (any, error) {
	attachmentID := vals.Get("TransitGatewayAttachmentId")
	if attachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	att, err := h.Backend.AcceptTransitGatewayVpcAttachment(attachmentID)
	if err != nil {
		return nil, err
	}

	return &acceptTransitGatewayVpcAttachmentResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		TransitGatewayVpcAtt: tgwVpcAttachmentToItem(att, h.Backend.TagsForResource(att.TransitGatewayAttachmentID)),
	}, nil
}

func (h *Handler) handleAcceptVpcEndpointConnections(vals url.Values, reqID string) (any, error) {
	serviceID := vals.Get("ServiceId")
	if serviceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	var endpointIDs []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("VpcEndpointId.%d", i))
		if id == "" {
			break
		}

		endpointIDs = append(endpointIDs, id)
	}

	accepted, err := h.Backend.AcceptVpcEndpointConnections(serviceID, endpointIDs)
	if err != nil {
		return nil, err
	}

	// AWS returns the connections in vpcEndpointConnectionSet.item on success.
	// The "unsuccessful" wrapper is empty because all connections were accepted.
	_ = accepted // connections were accepted; unsuccessful set is empty

	return &acceptVpcEndpointConnectionsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleAcceptVpcPeeringConnection(vals url.Values, reqID string) (any, error) {
	connectionID := vals.Get("VpcPeeringConnectionId")
	if connectionID == "" {
		return nil, fmt.Errorf("%w: VpcPeeringConnectionId is required", ErrInvalidParameter)
	}

	pc, err := h.Backend.AcceptVpcPeeringConnection(connectionID)
	if err != nil {
		return nil, err
	}

	return &acceptVpcPeeringConnectionResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		VpcPeeringConnection: toVpcPeeringConnectionItem(pc),
	}, nil
}

func (h *Handler) handleAdvertiseByoipCidr(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("Cidr")
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	entry, err := h.Backend.AdvertiseByoipCidr(cidr)
	if err != nil {
		return nil, err
	}

	return &advertiseByoipCidrResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ByoipCidr: byoipCidrItem{
			Cidr:          entry.Cidr,
			State:         entry.State,
			StatusMessage: entry.StatusMessage,
		},
	}, nil
}

func (h *Handler) handleAllocateHosts(vals url.Values, reqID string) (any, error) {
	az := vals.Get("AvailabilityZone")
	instanceType := vals.Get("InstanceType")

	count := 1
	if v := vals.Get("Quantity"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			count = n
		}
	}

	hosts, err := h.Backend.AllocateHosts(az, instanceType, count)
	if err != nil {
		return nil, err
	}

	resp := &allocateHostsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}

	for _, host := range hosts {
		resp.HostIDs.Items = append(resp.HostIDs.Items, host.HostID)
	}

	return resp, nil
}

// ---- Describe handlers for new resource types ----

type capacityReservationSet struct {
	Items []capacityReservationItem `xml:"item"`
}

type describeCapacityReservationsResponse struct {
	XMLName              xml.Name               `xml:"DescribeCapacityReservationsResponse"`
	Xmlns                string                 `xml:"xmlns,attr"`
	RequestID            string                 `xml:"requestId"`
	CapacityReservations capacityReservationSet `xml:"capacityReservationSet"`
}

func (h *Handler) handleDescribeCapacityReservations(vals url.Values, reqID string) (any, error) {
	var ids []string

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("CapacityReservationId.%d", i))
		if id == "" {
			break
		}

		ids = append(ids, id)
	}

	reservations := h.Backend.DescribeCapacityReservations(ids)

	resp := &describeCapacityReservationsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}

	for _, cr := range reservations {
		resp.CapacityReservations.Items = append(resp.CapacityReservations.Items,
			capacityReservationItem{
				CapacityReservationID:  cr.CapacityReservationID,
				InstanceType:           cr.InstanceType,
				AvailabilityZone:       cr.AvailabilityZone,
				OwnedBy:                cr.OwnedBy,
				State:                  cr.State,
				AvailableInstanceCount: cr.AvailableInstanceCount,
				TotalInstanceCount:     cr.TotalInstanceCount,
			})
	}

	return resp, nil
}

type byoipCidrSet struct {
	Items []byoipCidrItem `xml:"item"`
}

type describeByoipCidrsResponse struct {
	XMLName    xml.Name     `xml:"DescribeByoipCidrsResponse"`
	Xmlns      string       `xml:"xmlns,attr"`
	RequestID  string       `xml:"requestId"`
	ByoipCidrs byoipCidrSet `xml:"byoipCidrSet"`
}

func (h *Handler) handleDescribeByoipCidrs(vals url.Values, reqID string) (any, error) {
	state := vals.Get("State")

	cidrs := h.Backend.DescribeByoipCidrs(state)

	resp := &describeByoipCidrsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}

	for _, c := range cidrs {
		resp.ByoipCidrs.Items = append(resp.ByoipCidrs.Items, byoipCidrItem{
			Cidr:          c.Cidr,
			State:         c.State,
			StatusMessage: c.StatusMessage,
		})
	}

	return resp, nil
}

// hostPropertiesItem mirrors the real Host.HostProperties nested shape
// (ec2@v1.319.1 types.go's HostProperties): InstanceType and InstanceFamily
// live here, not as top-level Host fields.
type hostPropertiesItem struct {
	InstanceType   string `xml:"instanceType,omitempty"`
	InstanceFamily string `xml:"instanceFamily,omitempty"`
}

type hostItem struct {
	HostID           string             `xml:"hostId"`
	AvailabilityZone string             `xml:"availabilityZone"`
	State            string             `xml:"state"`
	OwnedBy          string             `xml:"ownerId,omitempty"`
	AutoPlacement    string             `xml:"autoPlacement,omitempty"`
	HostRecovery     string             `xml:"hostRecovery,omitempty"`
	HostMaintenance  string             `xml:"hostMaintenance,omitempty"`
	HostProperties   hostPropertiesItem `xml:"hostProperties"`
}

type hostSet struct {
	Items []hostItem `xml:"item"`
}

type describeHostsResponse struct {
	XMLName   xml.Name `xml:"DescribeHostsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Hosts     hostSet  `xml:"hostSet"`
}

func (h *Handler) handleDescribeHosts(vals url.Values, reqID string) (any, error) {
	var ids []string

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("HostId.%d", i))
		if id == "" {
			break
		}

		ids = append(ids, id)
	}

	hosts := h.Backend.DescribeHosts(ids)

	resp := &describeHostsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}

	for _, host := range hosts {
		resp.Hosts.Items = append(resp.Hosts.Items, hostItem{
			HostID:           host.HostID,
			AvailabilityZone: host.AvailabilityZone,
			State:            host.State,
			OwnedBy:          host.OwnedBy,
			AutoPlacement:    host.AutoPlacement,
			HostRecovery:     host.HostRecovery,
			HostMaintenance:  host.HostMaintenance,
			HostProperties: hostPropertiesItem{
				InstanceType:   host.InstanceType,
				InstanceFamily: host.InstanceFamily,
			},
		})
	}

	return resp, nil
}

type vpcPeeringConnectionSet struct {
	Items []vpcPeeringConnectionItem `xml:"item"`
}

type describeVpcPeeringConnectionsResponse struct {
	XMLName               xml.Name                `xml:"DescribeVpcPeeringConnectionsResponse"`
	Xmlns                 string                  `xml:"xmlns,attr"`
	RequestID             string                  `xml:"requestId"`
	VpcPeeringConnections vpcPeeringConnectionSet `xml:"vpcPeeringConnectionSet"`
}

func (h *Handler) handleDescribeVpcPeeringConnections(vals url.Values, reqID string) (any, error) {
	var ids []string

	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("VpcPeeringConnectionId.%d", i))
		if id == "" {
			break
		}

		ids = append(ids, id)
	}

	connections := h.Backend.DescribeVpcPeeringConnections(ids)

	resp := &describeVpcPeeringConnectionsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
	}

	for _, pc := range connections {
		resp.VpcPeeringConnections.Items = append(
			resp.VpcPeeringConnections.Items, toVpcPeeringConnectionItem(pc),
		)
	}

	return resp, nil
}
