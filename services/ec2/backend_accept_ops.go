package ec2

import (
	"errors"
	"fmt"
	"sort"
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
	AllocationID        string    `json:"allocationID,omitempty"`
	PublicIP            string    `json:"publicIP,omitempty"`
	TransferAccountID   string    `json:"transferAccountID,omitempty"`
	TransferOfferStatus string    `json:"transferOfferStatus,omitempty"`
}

// CapacityReservation represents an EC2 Capacity Reservation.
type CapacityReservation struct {
	CreateTime            time.Time `json:"createTime"`
	CapacityReservationID string    `json:"capacityReservationID,omitempty"`
	InstanceType          string    `json:"instanceType,omitempty"`
	AvailabilityZone      string    `json:"availabilityZone,omitempty"`
	OwnedBy               string    `json:"ownedBy,omitempty"`
	State                 string    `json:"state,omitempty"`
	// InstancePlatform is the OS platform reserved (e.g. "Linux/UNIX"). Populated
	// for Capacity Block purchases; empty for plain CreateCapacityReservation
	// calls that predate this field.
	InstancePlatform       string `json:"instancePlatform,omitempty"`
	AvailableInstanceCount int    `json:"availableInstanceCount,omitempty"`
	TotalInstanceCount     int    `json:"totalInstanceCount,omitempty"`
}

// ReservedInstancesExchange represents a completed reserved instances exchange.
type ReservedInstancesExchange struct {
	ExchangeID              string   `json:"exchangeID,omitempty"`
	Status                  string   `json:"status,omitempty"`
	ReservedInstanceIDs     []string `json:"reservedInstanceIDs,omitempty"`
	TargetReservedInstances []string `json:"targetReservedInstances,omitempty"`
}

// TransitGatewayMulticastDomainAssociation represents a multicast domain association.
type TransitGatewayMulticastDomainAssociation struct {
	TransitGatewayMulticastDomainID string `json:"transitGatewayMulticastDomainID,omitempty"`
	TransitGatewayAttachmentID      string `json:"transitGatewayAttachmentID,omitempty"`
	SubnetID                        string `json:"subnetID,omitempty"`
	State                           string `json:"state,omitempty"`
}

// TransitGatewayPeeringAttachment represents a TGW peering attachment.
type TransitGatewayPeeringAttachment struct {
	CreationTime               time.Time `json:"creationTime"`
	TransitGatewayAttachmentID string    `json:"transitGatewayAttachmentID,omitempty"`
	RequesterTransitGatewayID  string    `json:"requesterTransitGatewayID,omitempty"`
	AccepterTransitGatewayID   string    `json:"accepterTransitGatewayID,omitempty"`
	State                      string    `json:"state,omitempty"`
}

// TransitGatewayVpcAttachment represents a TGW VPC attachment.
type TransitGatewayVpcAttachment struct {
	CreationTime               time.Time `json:"creationTime"`
	TransitGatewayAttachmentID string    `json:"transitGatewayAttachmentID,omitempty"`
	TransitGatewayID           string    `json:"transitGatewayID,omitempty"`
	VpcID                      string    `json:"vpcID,omitempty"`
	State                      string    `json:"state,omitempty"`
	// SubnetIDs is the set of subnet IDs the attachment uses, managed via
	// ModifyTransitGatewayVpcAttachment's AddSubnetIds/RemoveSubnetIds.
	SubnetIDs []string `json:"subnetIDs,omitempty"`
}

// VpcEndpointConnection represents a VPC endpoint connection to a service.
type VpcEndpointConnection struct {
	CreationTime  time.Time `json:"creationTime"`
	ServiceID     string    `json:"serviceID,omitempty"`
	VpcEndpointID string    `json:"vpcEndpointID,omitempty"`
	State         string    `json:"state,omitempty"`
}

// VpcPeeringConnection represents a VPC peering connection.
type VpcPeeringConnection struct {
	ExpirationTime         time.Time `json:"expirationTime"`
	VpcPeeringConnectionID string    `json:"vpcPeeringConnectionID,omitempty"`
	RequesterVpcID         string    `json:"requesterVpcID,omitempty"`
	AccepterVpcID          string    `json:"accepterVpcID,omitempty"`
	State                  string    `json:"state,omitempty"`
}

// ByoipCidr represents a Bring Your Own IP (BYOIP) CIDR entry.
type ByoipCidr struct {
	Cidr          string `json:"cidr,omitempty"`
	State         string `json:"state,omitempty"`
	StatusMessage string `json:"statusMessage,omitempty"`
}

// Host represents a Dedicated Host.
type Host struct {
	HostID           string    `json:"hostID,omitempty"`
	InstanceType     string    `json:"instanceType,omitempty"`
	AvailabilityZone string    `json:"availabilityZone,omitempty"`
	State            string    `json:"state,omitempty"`
	AllocationTime   time.Time `json:"allocationTime"`
	OwnedBy          string    `json:"ownedBy,omitempty"`
}

// ---- Reset ----

// Reset clears all resource state in the backend, returning it to its initial state.
// All resource maps for original and new operations are re-created, and defaults
// (default VPC, subnet, security group) are re-populated.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Reset original resource maps.
	b.instances = make(map[string]*Instance)
	b.securityGroups = make(map[string]*SecurityGroup)
	b.vpcs = make(map[string]*VPC)
	b.subnets = make(map[string]*Subnet)
	b.keyPairs = make(map[string]*KeyPair)
	b.volumes = make(map[string]*Volume)
	b.addresses = make(map[string]*Address)
	b.internetGateways = make(map[string]*InternetGateway)
	b.routeTables = make(map[string]*RouteTable)
	b.natGateways = make(map[string]*NatGateway)
	b.networkInterfaces = make(map[string]*NetworkInterface)
	b.spotRequests = make(map[string]*SpotInstanceRequest)
	b.placementGroups = make(map[string]*PlacementGroup)
	b.images = make(map[string]*AMIStub)
	b.imageUsageReports = make(map[string]*ImageUsageReport)
	b.launchTemplates = make(map[string]*LaunchTemplate)
	b.vpcEndpoints = make(map[string]*VpcEndpoint)
	b.tags = make(map[string]map[string]string)
	b.instanceIDsByVPC = make(map[string]map[string]struct{})
	b.eniIDsByInstance = make(map[string]map[string]struct{})
	b.eniIDByAttachment = make(map[string]string)
	b.freePrivateIPs = nil
	b.nextPrivateIPIndex = 0
	b.nextElasticIPIndex = 0

	b.resetNewOpsMapsLocked()

	// Re-populate defaults (must be called without the lock held since it acquires its own).
	// Since we already hold the lock, populate inline.
	b.vpcs[vpcDefaultName] = &VPC{
		ID:        vpcDefaultName,
		CIDRBlock: "172.31.0.0/16",
		IsDefault: true,
	}
	b.subnets["subnet-default"] = &Subnet{
		ID:               "subnet-default",
		VPCID:            vpcDefaultName,
		CIDRBlock:        "172.31.0.0/20",
		AvailabilityZone: b.Region + "a",
		IsDefault:        true,
	}
	b.securityGroups["sg-default"] = &SecurityGroup{
		ID:          "sg-default",
		Name:        "default",
		Description: "default VPC security group",
		VPCID:       vpcDefaultName,
	}
}

// resetNewOpsMapsLocked re-initialises all "new operations" resource maps introduced
// after the original core set. Must be called with b.mu held.
func (b *InMemoryBackend) resetNewOpsMapsLocked() {
	b.addressTransfers = make(map[string]*AddressTransfer)
	b.capacityReservations = make(map[string]*CapacityReservation)
	b.reservedInstancesExchanges = make(map[string]*ReservedInstancesExchange)
	b.tgwMulticastDomainAssociations = make(map[string]*TransitGatewayMulticastDomainAssociation)
	b.tgwPeeringAttachments = make(map[string]*TransitGatewayPeeringAttachment)
	b.tgwVpcAttachments = make(map[string]*TransitGatewayVpcAttachment)
	b.vpcEndpointConnections = make(map[string]*VpcEndpointConnection)
	b.vpcPeeringConnections = make(map[string]*VpcPeeringConnection)
	b.byoipCidrs = make(map[string]*ByoipCidr)
	b.dedicatedHosts = make(map[string]*Host)
	b.snapshots = make(map[string]*Snapshot)
	b.networkACLs = make(map[string]*StoredNetworkACL)
	b.transitGateways = make(map[string]*TransitGateway)
	b.flowLogs = make(map[string]*FlowLog)
	b.dhcpOptionSets = make(map[string]*DhcpOptions)
	b.egressOnlyIGWs = make(map[string]*EgressOnlyInternetGateway)
	b.iamAssociations = make(map[string]*IamInstanceProfileAssociation)
	b.tgwRouteTables = make(map[string]*TransitGatewayRouteTable)
	b.tgwRoutes = make(map[string]*TransitGatewayRoute)
	b.tgwRTAssociations = make(map[string]*TransitGatewayRouteTableAssociation)
	b.tgwPolicyTables = make(map[string]*TransitGatewayPolicyTable)
	b.tgwPolicyTableAssociations = make(map[string]*TransitGatewayPolicyTableAssociation)
	b.tgwRouteTableAnnouncements = make(map[string]*TransitGatewayRouteTableAnnouncement)
	b.vpcCidrAssociations = make(map[string]*VpcCidrBlockAssociation)
	b.resetAdvancedNetworkingMapsLocked()
	b.resetIpamDiscoveryMapsLocked()
	b.resetIpamPolicyMapsLocked()
	b.resetBatch4MapsLocked()
	initTGWMulticastMaps(b)
	initVpcConfigMaps(b)
	initCapacityFamilyMaps(b)
	initVerifiedAccessExtMaps(b)
	initFpgaImageMaps(b)
	b.resetScheduledInstanceMapsLocked()
	b.resetIPPoolMapsLocked()
	b.resetAllowedImagesSettingsLocked()
	b.resetImageTasksLocked()
	b.resetUsageReportMapsLocked()
	b.instanceProductCodes = make(map[string][]string)
}

// resetBatch4MapsLocked re-initialises all batch4 resource maps.
// Must be called with b.mu held.
func (b *InMemoryBackend) resetBatch4MapsLocked() {
	b.managedPrefixLists = make(map[string]*ManagedPrefixList)
	b.clientVpnEndpoints = make(map[string]*ClientVpnEndpoint)
	b.tgwConnects = make(map[string]*TransitGatewayConnect)
	b.tgwConnectPeers = make(map[string]*TransitGatewayConnectPeer)
	b.tgwPrefixListRefs = make(map[string]*TransitGatewayPrefixListReference)
	b.verifiedAccessEndpoints = make(map[string]*VerifiedAccessEndpoint)
	b.verifiedAccessGroups = make(map[string]*VerifiedAccessGroup)
	b.verifiedAccessInstances = make(map[string]*VerifiedAccessInstance)
	b.verifiedAccessTrustProviders = make(map[string]*VerifiedAccessTrustProvider)
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
// The key is the PublicIP field of the transfer. The value is deep-copied.
func (b *InMemoryBackend) AddAddressTransferInternal(t *AddressTransfer) {
	b.mu.Lock("AddAddressTransferInternal")
	defer b.mu.Unlock()

	cp := *t
	b.addressTransfers[cp.PublicIP] = &cp
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
	cr.OwnedBy = b.AccountID
	cp := *cr

	return &cp, nil
}

// AddCapacityReservationInternal inserts a CapacityReservation directly (for seeding in tests).
// OwnedBy is populated from b.AccountID if unset. The value is deep-copied.
func (b *InMemoryBackend) AddCapacityReservationInternal(cr *CapacityReservation) {
	b.mu.Lock("AddCapacityReservationInternal")
	defer b.mu.Unlock()

	cp := *cr
	if cp.OwnedBy == "" {
		cp.OwnedBy = b.AccountID
	}

	if cp.CreateTime.IsZero() {
		cp.CreateTime = time.Now()
	}

	b.capacityReservations[cp.CapacityReservationID] = &cp
}

// DescribeCapacityReservations returns capacity reservations sorted by ID.
// If ids is non-empty, only reservations matching those IDs are returned.
func (b *InMemoryBackend) DescribeCapacityReservations(ids []string) []*CapacityReservation {
	b.mu.RLock("DescribeCapacityReservations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var result []*CapacityReservation

	for _, cr := range b.capacityReservations {
		if len(idSet) > 0 && !idSet[cr.CapacityReservationID] {
			continue
		}

		cp := *cr
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityReservationID < result[j].CapacityReservationID
	})

	return result
}

// ---- AcceptReservedInstancesExchangeQuote ----

// AcceptReservedInstancesExchangeQuote accepts an exchange quote for reserved instances,
// creating a new exchange record with "successful" status.
func (b *InMemoryBackend) AcceptReservedInstancesExchangeQuote(
	reservedInstanceIDs []string,
) (*ReservedInstancesExchange, error) {
	if len(reservedInstanceIDs) == 0 {
		return nil, fmt.Errorf(
			"%w: at least one ReservedInstanceId is required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("AcceptReservedInstancesExchangeQuote")
	defer b.mu.Unlock()

	exchangeID := "riex-" + uuid.New().String()[:17]
	targetID := "ri-" + uuid.New().String()[:17]

	ids := make([]string, len(reservedInstanceIDs))
	copy(ids, reservedInstanceIDs)

	exchange := &ReservedInstancesExchange{
		ExchangeID:              exchangeID,
		ReservedInstanceIDs:     ids,
		TargetReservedInstances: []string{targetID},
		Status:                  "successful",
	}

	b.reservedInstancesExchanges[exchangeID] = exchange

	cp := *exchange
	cp.ReservedInstanceIDs = make([]string, len(exchange.ReservedInstanceIDs))
	copy(cp.ReservedInstanceIDs, exchange.ReservedInstanceIDs)
	cp.TargetReservedInstances = make([]string, len(exchange.TargetReservedInstances))
	copy(cp.TargetReservedInstances, exchange.TargetReservedInstances)

	return &cp, nil
}

// ---- AcceptTransitGatewayMulticastDomainAssociations ----

// AcceptTransitGatewayMulticastDomainAssociations accepts a request to associate
// subnets with a transit gateway multicast domain.
// Returns a non-nil (possibly empty) slice of associations.
func (b *InMemoryBackend) AcceptTransitGatewayMulticastDomainAssociations(
	transitGatewayMulticastDomainID, transitGatewayAttachmentID string,
	subnetIDs []string,
) ([]*TransitGatewayMulticastDomainAssociation, error) {
	if transitGatewayMulticastDomainID == "" {
		return nil, fmt.Errorf(
			"%w: TransitGatewayMulticastDomainId is required",
			ErrInvalidParameter,
		)
	}

	if transitGatewayAttachmentID == "" {
		return nil, fmt.Errorf("%w: TransitGatewayAttachmentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptTransitGatewayMulticastDomainAssociations")
	defer b.mu.Unlock()

	// Always return a non-nil slice.
	assocs := make([]*TransitGatewayMulticastDomainAssociation, 0, len(subnetIDs))

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

// AddTGWMulticastDomainAssociationInternal inserts a TGW multicast domain association
// directly (for seeding in tests). The value is deep-copied.
func (b *InMemoryBackend) AddTGWMulticastDomainAssociationInternal(
	assoc *TransitGatewayMulticastDomainAssociation,
) {
	b.mu.Lock("AddTGWMulticastDomainAssociationInternal")
	defer b.mu.Unlock()

	cp := *assoc
	key := cp.TransitGatewayMulticastDomainID + ":" + cp.SubnetID
	b.tgwMulticastDomainAssociations[key] = &cp
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
		return nil, fmt.Errorf(
			"%w: %s",
			ErrTransitGatewayAttachmentNotFound,
			transitGatewayAttachmentID,
		)
	}

	att.State = stateAvailable
	cp := *att

	return &cp, nil
}

// AddTGWPeeringAttachmentInternal inserts a TGW peering attachment (for seeding in tests).
// CreationTime is populated if zero. The value is deep-copied.
func (b *InMemoryBackend) AddTGWPeeringAttachmentInternal(att *TransitGatewayPeeringAttachment) {
	b.mu.Lock("AddTGWPeeringAttachmentInternal")
	defer b.mu.Unlock()

	cp := *att
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now()
	}

	b.tgwPeeringAttachments[cp.TransitGatewayAttachmentID] = &cp
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
		return nil, fmt.Errorf(
			"%w: %s",
			ErrTransitGatewayAttachmentNotFound,
			transitGatewayAttachmentID,
		)
	}

	att.State = stateAvailable
	cp := *att

	return &cp, nil
}

// AddTGWVpcAttachmentInternal inserts a TGW VPC attachment (for seeding in tests).
// CreationTime is populated if zero. The value is deep-copied.
func (b *InMemoryBackend) AddTGWVpcAttachmentInternal(att *TransitGatewayVpcAttachment) {
	b.mu.Lock("AddTGWVpcAttachmentInternal")
	defer b.mu.Unlock()

	cp := *att
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now()
	}

	b.tgwVpcAttachments[cp.TransitGatewayAttachmentID] = &cp
}

// ---- AcceptVpcEndpointConnections ----

// AcceptVpcEndpointConnections accepts VPC endpoint connections to the given service,
// transitioning each endpoint's state to "available".
// Returns a non-nil (possibly empty) slice of accepted connections.
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

	// Always return a non-nil slice.
	accepted := make([]*VpcEndpointConnection, 0, len(vpcEndpointIDs))

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

// AddVpcEndpointConnectionInternal inserts a VPC endpoint connection directly
// (for seeding in tests). The value is deep-copied.
func (b *InMemoryBackend) AddVpcEndpointConnectionInternal(conn *VpcEndpointConnection) {
	b.mu.Lock("AddVpcEndpointConnectionInternal")
	defer b.mu.Unlock()

	cp := *conn
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now()
	}

	key := cp.ServiceID + ":" + cp.VpcEndpointID
	b.vpcEndpointConnections[key] = &cp
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
// ExpirationTime is set to 7 days from now if zero. The value is deep-copied.
func (b *InMemoryBackend) AddVpcPeeringConnectionInternal(pc *VpcPeeringConnection) {
	b.mu.Lock("AddVpcPeeringConnectionInternal")
	defer b.mu.Unlock()

	cp := *pc
	if cp.ExpirationTime.IsZero() {
		cp.ExpirationTime = time.Now().Add(7 * 24 * time.Hour)
	}

	b.vpcPeeringConnections[cp.VpcPeeringConnectionID] = &cp
}

// DescribeVpcPeeringConnections returns VPC peering connections sorted by ID.
// If ids is non-empty, only connections matching those IDs are returned.
func (b *InMemoryBackend) DescribeVpcPeeringConnections(ids []string) []*VpcPeeringConnection {
	b.mu.RLock("DescribeVpcPeeringConnections")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var result []*VpcPeeringConnection

	for _, pc := range b.vpcPeeringConnections {
		if len(idSet) > 0 && !idSet[pc.VpcPeeringConnectionID] {
			continue
		}

		cp := *pc
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].VpcPeeringConnectionID < result[j].VpcPeeringConnectionID
	})

	return result
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
			State: stateByoipAdvertised,
		}
		b.byoipCidrs[cidr] = entry
	} else {
		entry.State = stateByoipAdvertised
	}

	cp := *entry

	return &cp, nil
}

// AddByoipCidrInternal inserts a ByoipCidr directly (for seeding in tests).
// The value is deep-copied.
func (b *InMemoryBackend) AddByoipCidrInternal(cidr *ByoipCidr) {
	b.mu.Lock("AddByoipCidrInternal")
	defer b.mu.Unlock()

	cp := *cidr
	b.byoipCidrs[cp.Cidr] = &cp
}

// DescribeByoipCidrs returns BYOIP CIDRs sorted by CIDR string.
// If state is non-empty, only CIDRs in that state are returned.
func (b *InMemoryBackend) DescribeByoipCidrs(state string) []*ByoipCidr {
	b.mu.RLock("DescribeByoipCidrs")
	defer b.mu.RUnlock()

	var result []*ByoipCidr

	for _, c := range b.byoipCidrs {
		if state != "" && c.State != state {
			continue
		}

		cp := *c
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Cidr < result[j].Cidr
	})

	return result
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

	// Always return a non-nil slice.
	hosts := make([]*Host, 0, hostCount)

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

// DescribeHosts returns dedicated hosts sorted by host ID.
// If ids is non-empty, only hosts matching those IDs are returned.
func (b *InMemoryBackend) DescribeHosts(ids []string) []*Host {
	b.mu.RLock("DescribeHosts")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	var result []*Host

	for _, h := range b.dedicatedHosts {
		if len(idSet) > 0 && !idSet[h.HostID] {
			continue
		}

		cp := *h
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].HostID < result[j].HostID
	})

	return result
}
