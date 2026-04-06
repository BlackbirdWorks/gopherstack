package ec2

import (
	"encoding/json"
	"log/slog"
)

// snapTGWMcastAssoc is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWMcastAssoc = TransitGatewayMulticastDomainAssociation

// snapRIExchange is a type alias used in backendSnapshot to keep line lengths manageable.
type snapRIExchange = ReservedInstancesExchange

// snapTGWPeeringAtt is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWPeeringAtt = TransitGatewayPeeringAttachment

// snapTGWVpcAtt is a type alias used in backendSnapshot to keep line lengths manageable.
type snapTGWVpcAtt = TransitGatewayVpcAttachment

type backendSnapshot struct {
	RouteTables                    map[string]*RouteTable            `json:"routeTables"`
	NetworkInterfaces              map[string]*NetworkInterface      `json:"networkInterfaces"`
	VPCs                           map[string]*VPC                   `json:"vpcs"`
	NatGateways                    map[string]*NatGateway            `json:"natGateways"`
	KeyPairs                       map[string]*KeyPair               `json:"keyPairs"`
	Volumes                        map[string]*Volume                `json:"volumes"`
	Addresses                      map[string]*Address               `json:"addresses"`
	InternetGateways               map[string]*InternetGateway       `json:"internetGateways"`
	SecurityGroups                 map[string]*SecurityGroup         `json:"securityGroups"`
	Instances                      map[string]*Instance              `json:"instances"`
	Subnets                        map[string]*Subnet                `json:"subnets"`
	SpotRequests                   map[string]*SpotInstanceRequest   `json:"spotRequests"`
	PlacementGroups                map[string]*PlacementGroup        `json:"placementGroups"`
	Tags                           map[string]map[string]string      `json:"tags"`
	AddressTransfers               map[string]*AddressTransfer       `json:"addressTransfers,omitempty"`
	CapacityReservations           map[string]*CapacityReservation   `json:"capacityReservations,omitempty"`
	ReservedInstancesExchanges     map[string]*snapRIExchange        `json:"reservedInstancesExchanges,omitempty"`
	TGWMulticastDomainAssociations map[string]*snapTGWMcastAssoc     `json:"tgwMulticastDomainAssociations,omitempty"`
	TGWPeeringAttachments          map[string]*snapTGWPeeringAtt     `json:"tgwPeeringAttachments,omitempty"`
	TGWVpcAttachments              map[string]*snapTGWVpcAtt         `json:"tgwVpcAttachments,omitempty"`
	VpcEndpointConnections         map[string]*VpcEndpointConnection `json:"vpcEndpointConnections,omitempty"`
	VpcPeeringConnections          map[string]*VpcPeeringConnection  `json:"vpcPeeringConnections,omitempty"`
	ByoipCidrs                     map[string]*ByoipCidr             `json:"byoipCidrs,omitempty"`
	DedicatedHosts                 map[string]*Host                  `json:"dedicatedHosts,omitempty"`
	AccountID                      string                            `json:"accountID"`
	Region                         string                            `json:"region"`
	FreePrivateIPs                 []string                          `json:"freePrivateIPs,omitempty"`
	NextPrivateIPIndex             int                               `json:"nextPrivateIPIndex"`
	NextElasticIPIndex             int                               `json:"nextElasticIPIndex"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:                      b.instances,
		SecurityGroups:                 b.securityGroups,
		VPCs:                           b.vpcs,
		Subnets:                        b.subnets,
		KeyPairs:                       b.keyPairs,
		Volumes:                        b.volumes,
		Addresses:                      b.addresses,
		InternetGateways:               b.internetGateways,
		RouteTables:                    b.routeTables,
		NatGateways:                    b.natGateways,
		NetworkInterfaces:              b.networkInterfaces,
		SpotRequests:                   b.spotRequests,
		PlacementGroups:                b.placementGroups,
		Tags:                           b.tags,
		AddressTransfers:               b.addressTransfers,
		CapacityReservations:           b.capacityReservations,
		ReservedInstancesExchanges:     b.reservedInstancesExchanges,
		TGWMulticastDomainAssociations: b.tgwMulticastDomainAssociations,
		TGWPeeringAttachments:          b.tgwPeeringAttachments,
		TGWVpcAttachments:              b.tgwVpcAttachments,
		VpcEndpointConnections:         b.vpcEndpointConnections,
		VpcPeeringConnections:          b.vpcPeeringConnections,
		ByoipCidrs:                     b.byoipCidrs,
		DedicatedHosts:                 b.dedicatedHosts,
		FreePrivateIPs:                 b.freePrivateIPs,
		AccountID:                      b.AccountID,
		Region:                         b.Region,
		NextPrivateIPIndex:             b.nextPrivateIPIndex,
		NextElasticIPIndex:             b.nextElasticIPIndex,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("ec2: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.initMissingMaps()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.instances = snap.Instances
	b.securityGroups = snap.SecurityGroups
	b.vpcs = snap.VPCs
	b.subnets = snap.Subnets
	b.keyPairs = snap.KeyPairs
	b.volumes = snap.Volumes
	b.addresses = snap.Addresses
	b.internetGateways = snap.InternetGateways
	b.routeTables = snap.RouteTables
	b.natGateways = snap.NatGateways
	b.networkInterfaces = snap.NetworkInterfaces
	b.spotRequests = snap.SpotRequests
	b.placementGroups = snap.PlacementGroups
	b.tags = snap.Tags
	b.addressTransfers = snap.AddressTransfers
	b.capacityReservations = snap.CapacityReservations
	b.reservedInstancesExchanges = snap.ReservedInstancesExchanges
	b.tgwMulticastDomainAssociations = snap.TGWMulticastDomainAssociations
	b.tgwPeeringAttachments = snap.TGWPeeringAttachments
	b.tgwVpcAttachments = snap.TGWVpcAttachments
	b.vpcEndpointConnections = snap.VpcEndpointConnections
	b.vpcPeeringConnections = snap.VpcPeeringConnections
	b.byoipCidrs = snap.ByoipCidrs
	b.dedicatedHosts = snap.DedicatedHosts
	b.freePrivateIPs = snap.FreePrivateIPs
	b.AccountID = snap.AccountID
	b.Region = snap.Region
	b.nextPrivateIPIndex = snap.NextPrivateIPIndex
	b.nextElasticIPIndex = snap.NextElasticIPIndex

	return nil
}

// initMissingMaps ensures all map fields in the snapshot are non-nil.
// This prevents nil-map panics when the snapshot was created from a backend
// that never populated a particular resource type.
func (s *backendSnapshot) initMissingMaps() {
	s.initCoreMaps()
	s.initNewOpsMaps()
}

// initCoreMaps initialises the original map fields.
func (s *backendSnapshot) initCoreMaps() {
	if s.Instances == nil {
		s.Instances = make(map[string]*Instance)
	}

	if s.SecurityGroups == nil {
		s.SecurityGroups = make(map[string]*SecurityGroup)
	}

	if s.VPCs == nil {
		s.VPCs = make(map[string]*VPC)
	}

	if s.Subnets == nil {
		s.Subnets = make(map[string]*Subnet)
	}

	if s.KeyPairs == nil {
		s.KeyPairs = make(map[string]*KeyPair)
	}

	if s.Volumes == nil {
		s.Volumes = make(map[string]*Volume)
	}

	if s.Addresses == nil {
		s.Addresses = make(map[string]*Address)
	}

	if s.InternetGateways == nil {
		s.InternetGateways = make(map[string]*InternetGateway)
	}

	if s.RouteTables == nil {
		s.RouteTables = make(map[string]*RouteTable)
	}

	if s.NatGateways == nil {
		s.NatGateways = make(map[string]*NatGateway)
	}

	if s.NetworkInterfaces == nil {
		s.NetworkInterfaces = make(map[string]*NetworkInterface)
	}

	if s.SpotRequests == nil {
		s.SpotRequests = make(map[string]*SpotInstanceRequest)
	}

	if s.PlacementGroups == nil {
		s.PlacementGroups = make(map[string]*PlacementGroup)
	}

	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}
}

// initNewOpsMaps initialises the map fields added for the new Accept/Advertise/Allocate operations.
func (s *backendSnapshot) initNewOpsMaps() {
	if s.AddressTransfers == nil {
		s.AddressTransfers = make(map[string]*AddressTransfer)
	}

	if s.CapacityReservations == nil {
		s.CapacityReservations = make(map[string]*CapacityReservation)
	}

	if s.ReservedInstancesExchanges == nil {
		s.ReservedInstancesExchanges = make(map[string]*ReservedInstancesExchange)
	}

	if s.TGWMulticastDomainAssociations == nil {
		s.TGWMulticastDomainAssociations = make(map[string]*TransitGatewayMulticastDomainAssociation)
	}

	if s.TGWPeeringAttachments == nil {
		s.TGWPeeringAttachments = make(map[string]*TransitGatewayPeeringAttachment)
	}

	if s.TGWVpcAttachments == nil {
		s.TGWVpcAttachments = make(map[string]*TransitGatewayVpcAttachment)
	}

	if s.VpcEndpointConnections == nil {
		s.VpcEndpointConnections = make(map[string]*VpcEndpointConnection)
	}

	if s.VpcPeeringConnections == nil {
		s.VpcPeeringConnections = make(map[string]*VpcPeeringConnection)
	}

	if s.ByoipCidrs == nil {
		s.ByoipCidrs = make(map[string]*ByoipCidr)
	}

	if s.DedicatedHosts == nil {
		s.DedicatedHosts = make(map[string]*Host)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Snapshot support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
// It type-asserts the backend to check for Restore support so that alternative
// backend implementations that do not persist state still compile.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
