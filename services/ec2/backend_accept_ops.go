package ec2

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ---- errors for new operations ----

var (
	// ErrAddressTransferNotFound is returned when an address transfer is not found.
	ErrAddressTransferNotFound = errors.New("InvalidAddressTransfer.NotFound")
	// ErrCapacityReservationNotFound is returned when a capacity reservation is not found.
	ErrCapacityReservationNotFound = errors.New("InvalidCapacityReservationId.NotFound")
	// ErrReservedInstancesNotFound is returned when reserved instances are not found.
	ErrReservedInstancesNotFound = errors.New("InvalidReservedInstancesId.NotFound")
	// ErrTransitGatewayAttachmentNotFound is returned when a TGW attachment is not found.
	ErrTransitGatewayAttachmentNotFound = errors.New("InvalidTransitGatewayAttachmentID.NotFound")
	// ErrVpcPeeringConnectionNotFound is returned when a VPC peering connection is not found.
	ErrVpcPeeringConnectionNotFound = errors.New("InvalidVpcPeeringConnectionID.NotFound")
	// ErrVpcEndpointNotFound is returned when a VPC endpoint service is not found.
	ErrVpcEndpointNotFound = errors.New("InvalidVpcEndpointService.NotFound")
	// ErrByoipCidrNotFound is returned when a BYOIP CIDR is not found.
	ErrByoipCidrNotFound = errors.New("InvalidByoipCidr.NotFound")
	// ErrHostNotFound is returned when a dedicated host is not found.
	ErrHostNotFound = errors.New("InvalidHostID.NotFound")
)

// ---- models ----

// AddressTransfer represents a pending Elastic IP address transfer.
type AddressTransfer struct {
	TransferOfferExpiry time.Time `json:"transferOfferExpiry"`
	AllocationID        string    `json:"allocationID"`
	PublicIP            string    `json:"publicIP"`
	TransferAccountID   string    `json:"transferAccountID"`
	TransferOfferStatus string    `json:"transferOfferStatus"`
}

// CapacityReservation represents an EC2 Capacity Reservation.
type CapacityReservation struct {
	CreateTime             time.Time `json:"createTime"`
	CapacityReservationID  string    `json:"capacityReservationID"`
	InstanceType           string    `json:"instanceType"`
	AvailabilityZone       string    `json:"availabilityZone"`
	OwnedBy                string    `json:"ownedBy"`
	State                  string    `json:"state"`
	AvailableInstanceCount int       `json:"availableInstanceCount"`
	TotalInstanceCount     int       `json:"totalInstanceCount"`
}

// ReservedInstancesExchange represents a completed reserved instances exchange.
type ReservedInstancesExchange struct {
	ExchangeID              string   `json:"exchangeID"`
	Status                  string   `json:"status"`
	ReservedInstanceIDs     []string `json:"reservedInstanceIDs"`
	TargetReservedInstances []string `json:"targetReservedInstances"`
}

// TransitGatewayMulticastDomainAssociation represents a multicast domain association.
type TransitGatewayMulticastDomainAssociation struct {
	TransitGatewayMulticastDomainID string `json:"transitGatewayMulticastDomainID"`
	TransitGatewayAttachmentID      string `json:"transitGatewayAttachmentID"`
	SubnetID                        string `json:"subnetID"`
	State                           string `json:"state"`
}

// TransitGatewayPeeringAttachment represents a TGW peering attachment.
type TransitGatewayPeeringAttachment struct {
	CreationTime               time.Time `json:"creationTime"`
	TransitGatewayAttachmentID string    `json:"transitGatewayAttachmentID"`
	RequesterTransitGatewayID  string    `json:"requesterTransitGatewayID"`
	AccepterTransitGatewayID   string    `json:"accepterTransitGatewayID"`
	State                      string    `json:"state"`
}

// TransitGatewayVpcAttachment represents a TGW VPC attachment.
type TransitGatewayVpcAttachment struct {
	CreationTime               time.Time `json:"creationTime"`
	TransitGatewayAttachmentID string    `json:"transitGatewayAttachmentID"`
	TransitGatewayID           string    `json:"transitGatewayID"`
	VpcID                      string    `json:"vpcID"`
	State                      string    `json:"state"`
}

// VpcEndpointConnection represents a VPC endpoint connection to a service.
type VpcEndpointConnection struct {
	CreationTime  time.Time `json:"creationTime"`
	ServiceID     string    `json:"serviceID"`
	VpcEndpointID string    `json:"vpcEndpointID"`
	State         string    `json:"state"`
}

// VpcPeeringConnection represents a VPC peering connection.
type VpcPeeringConnection struct {
	ExpirationTime         time.Time `json:"expirationTime"`
	VpcPeeringConnectionID string    `json:"vpcPeeringConnectionID"`
	RequesterVpcID         string    `json:"requesterVpcID"`
	AccepterVpcID          string    `json:"accepterVpcID"`
	State                  string    `json:"state"`
}

// ByoipCidr represents a Bring Your Own IP (BYOIP) CIDR entry.
type ByoipCidr struct {
	Cidr          string `json:"cidr"`
	State         string `json:"state"`
	StatusMessage string `json:"statusMessage"`
}

// Host represents a Dedicated Host.
type Host struct {
	HostID           string    `json:"hostID"`
	InstanceType     string    `json:"instanceType"`
	AvailabilityZone string    `json:"availabilityZone"`
	State            string    `json:"state"`
	AllocationTime   time.Time `json:"allocationTime"`
	OwnedBy          string    `json:"ownedBy"`
}

// ---- AcceptAddressTransfer ----

// AcceptAddressTransfer accepts a pending Elastic IP address transfer.
// It looks up a pending transfer for the given public IP address and transitions
// its status to "accepted".
func (b *InMemoryBackend) AcceptAddressTransfer(address string) (*AddressTransfer, error) {
	if address == "" {
		return nil, fmt.Errorf("%w: Address is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptAddressTransfer")
	defer b.mu.Unlock()

	transfer, ok := b.addressTransfers[address]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAddressTransferNotFound, address)
	}

	transfer.TransferOfferStatus = "accepted"
	cp := *transfer

	return &cp, nil
}

// AddAddressTransferInternal inserts an AddressTransfer directly (for seeding in tests).
// The key is the PublicIP field of the transfer.
func (b *InMemoryBackend) AddAddressTransferInternal(t *AddressTransfer) {
	b.mu.Lock("AddAddressTransferInternal")
	defer b.mu.Unlock()

	b.addressTransfers[t.PublicIP] = t
}

// ---- AcceptCapacityReservationBillingOwnership ----

// AcceptCapacityReservationBillingOwnership accepts the billing ownership of a
// capacity reservation, transitioning it to "active" state.
func (b *InMemoryBackend) AcceptCapacityReservationBillingOwnership(
	capacityReservationID string,
) (*CapacityReservation, error) {
	if capacityReservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptCapacityReservationBillingOwnership")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations[capacityReservationID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, capacityReservationID)
	}

	cr.State = stateActive
	cp := *cr

	return &cp, nil
}

// AddCapacityReservationInternal inserts a CapacityReservation directly (for seeding in tests).
func (b *InMemoryBackend) AddCapacityReservationInternal(cr *CapacityReservation) {
	b.mu.Lock("AddCapacityReservationInternal")
	defer b.mu.Unlock()

	b.capacityReservations[cr.CapacityReservationID] = cr
}

// ---- AcceptReservedInstancesExchangeQuote ----

// AcceptReservedInstancesExchangeQuote accepts an exchange quote for reserved instances,
// creating a new exchange record with "successful" status.
func (b *InMemoryBackend) AcceptReservedInstancesExchangeQuote(
	reservedInstanceIDs []string,
) (*ReservedInstancesExchange, error) {
	if len(reservedInstanceIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one ReservedInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptReservedInstancesExchangeQuote")
	defer b.mu.Unlock()

	exchangeID := "riex-" + uuid.New().String()[:17]
	targetID := "ri-" + uuid.New().String()[:17]

	exchange := &ReservedInstancesExchange{
		ExchangeID:              exchangeID,
		ReservedInstanceIDs:     reservedInstanceIDs,
		TargetReservedInstances: []string{targetID},
		Status:                  "successful",
	}

	b.reservedInstancesExchanges[exchangeID] = exchange

	cp := *exchange

	return &cp, nil
}

// ---- AcceptTransitGatewayMulticastDomainAssociations ----

// AcceptTransitGatewayMulticastDomainAssociations accepts a request to associate
// subnets with a transit gateway multicast domain.
func (b *InMemoryBackend) AcceptTransitGatewayMulticastDomainAssociations(
	transitGatewayMulticastDomainID, transitGatewayAttachmentID string,
	subnetIDs []string,
) ([]*TransitGatewayMulticastDomainAssociation, error) {
	if transitGatewayMulticastDomainID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayMulticastDomainId is required", ErrInvalidParameter)
	}

	if transitGatewayAttachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptTransitGatewayMulticastDomainAssociations")
	defer b.mu.Unlock()

	var assocs []*TransitGatewayMulticastDomainAssociation

	for _, subnetID := range subnetIDs {
		key := transitGatewayMulticastDomainID + ":" + subnetID
		assoc := &TransitGatewayMulticastDomainAssociation{
			TransitGatewayMulticastDomainID: transitGatewayMulticastDomainID,
			TransitGatewayAttachmentID:      transitGatewayAttachmentID,
			SubnetID:                        subnetID,
			State:                           "associated",
		}

		b.tgwMulticastDomainAssociations[key] = assoc

		cp := *assoc
		assocs = append(assocs, &cp)
	}

	return assocs, nil
}

// ---- AcceptTransitGatewayPeeringAttachment ----

// AcceptTransitGatewayPeeringAttachment accepts a TGW peering attachment,
// transitioning its state to "available".
func (b *InMemoryBackend) AcceptTransitGatewayPeeringAttachment(
	transitGatewayAttachmentID string,
) (*TransitGatewayPeeringAttachment, error) {
	if transitGatewayAttachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptTransitGatewayPeeringAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwPeeringAttachments[transitGatewayAttachmentID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, transitGatewayAttachmentID)
	}

	att.State = stateAvailable
	cp := *att

	return &cp, nil
}

// AddTGWPeeringAttachmentInternal inserts a TGW peering attachment (for seeding in tests).
func (b *InMemoryBackend) AddTGWPeeringAttachmentInternal(att *TransitGatewayPeeringAttachment) {
	b.mu.Lock("AddTGWPeeringAttachmentInternal")
	defer b.mu.Unlock()

	b.tgwPeeringAttachments[att.TransitGatewayAttachmentID] = att
}

// ---- AcceptTransitGatewayVpcAttachment ----

// AcceptTransitGatewayVpcAttachment accepts a TGW VPC attachment,
// transitioning its state to "available".
func (b *InMemoryBackend) AcceptTransitGatewayVpcAttachment(
	transitGatewayAttachmentID string,
) (*TransitGatewayVpcAttachment, error) {
	if transitGatewayAttachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptTransitGatewayVpcAttachment")
	defer b.mu.Unlock()

	att, ok := b.tgwVpcAttachments[transitGatewayAttachmentID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTransitGatewayAttachmentNotFound, transitGatewayAttachmentID)
	}

	att.State = stateAvailable
	cp := *att

	return &cp, nil
}

// AddTGWVpcAttachmentInternal inserts a TGW VPC attachment (for seeding in tests).
func (b *InMemoryBackend) AddTGWVpcAttachmentInternal(att *TransitGatewayVpcAttachment) {
	b.mu.Lock("AddTGWVpcAttachmentInternal")
	defer b.mu.Unlock()

	b.tgwVpcAttachments[att.TransitGatewayAttachmentID] = att
}

// ---- AcceptVpcEndpointConnections ----

// AcceptVpcEndpointConnections accepts VPC endpoint connections to the given service,
// transitioning each endpoint's state to "available".
func (b *InMemoryBackend) AcceptVpcEndpointConnections(
	serviceID string,
	vpcEndpointIDs []string,
) ([]*VpcEndpointConnection, error) {
	if serviceID == "" {
		return nil, fmt.Errorf("%w: ServiceId is required", ErrInvalidParameter)
	}

	if len(vpcEndpointIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one VpcEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptVpcEndpointConnections")
	defer b.mu.Unlock()

	var accepted []*VpcEndpointConnection

	for _, epID := range vpcEndpointIDs {
		key := serviceID + ":" + epID
		conn, ok := b.vpcEndpointConnections[key]

		if !ok {
			conn = &VpcEndpointConnection{
				ServiceID:     serviceID,
				VpcEndpointID: epID,
				CreationTime:  time.Now(),
			}
			b.vpcEndpointConnections[key] = conn
		}

		conn.State = stateAvailable
		cp := *conn
		accepted = append(accepted, &cp)
	}

	return accepted, nil
}

// ---- AcceptVpcPeeringConnection ----

// AcceptVpcPeeringConnection accepts a VPC peering connection, transitioning
// its state to "active".
func (b *InMemoryBackend) AcceptVpcPeeringConnection(
	vpcPeeringConnectionID string,
) (*VpcPeeringConnection, error) {
	if vpcPeeringConnectionID == "" {
		return nil, fmt.Errorf("%w: VpcPeeringConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptVpcPeeringConnection")
	defer b.mu.Unlock()

	pc, ok := b.vpcPeeringConnections[vpcPeeringConnectionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVpcPeeringConnectionNotFound, vpcPeeringConnectionID)
	}

	pc.State = stateActive
	cp := *pc

	return &cp, nil
}

// AddVpcPeeringConnectionInternal inserts a VPC peering connection (for seeding in tests).
func (b *InMemoryBackend) AddVpcPeeringConnectionInternal(pc *VpcPeeringConnection) {
	b.mu.Lock("AddVpcPeeringConnectionInternal")
	defer b.mu.Unlock()

	b.vpcPeeringConnections[pc.VpcPeeringConnectionID] = pc
}

// ---- AdvertiseByoipCidr ----

// AdvertiseByoipCidr marks a BYOIP CIDR as advertised.
// If the CIDR is not yet tracked, it is created in "advertised" state.
func (b *InMemoryBackend) AdvertiseByoipCidr(cidr string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("AdvertiseByoipCidr")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs[cidr]
	if !ok {
		entry = &ByoipCidr{
			Cidr:  cidr,
			State: "advertised",
		}
		b.byoipCidrs[cidr] = entry
	} else {
		entry.State = "advertised"
	}

	cp := *entry

	return &cp, nil
}

// ---- AllocateHosts ----

// AllocateHosts allocates one or more Dedicated Hosts in the given availability zone.
func (b *InMemoryBackend) AllocateHosts(
	availabilityZone, instanceType string,
	hostCount int,
) ([]*Host, error) {
	if availabilityZone == "" {
		return nil, fmt.Errorf("%w: AvailabilityZone is required", ErrInvalidParameter)
	}

	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	if hostCount < 1 {
		hostCount = 1
	}

	b.mu.Lock("AllocateHosts")
	defer b.mu.Unlock()

	var hosts []*Host

	for range hostCount {
		host := &Host{
			HostID:           "h-" + uuid.New().String()[:17],
			InstanceType:     instanceType,
			AvailabilityZone: availabilityZone,
			State:            stateAvailable,
			AllocationTime:   time.Now(),
			OwnedBy:          b.AccountID,
		}

		b.dedicatedHosts[host.HostID] = host

		cp := *host
		hosts = append(hosts, &cp)
	}

	return hosts, nil
}
