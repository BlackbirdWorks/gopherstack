package ec2

import (
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// backend_secondary_net.go implements the Secondary Network / Secondary Subnet /
// Secondary Interface family (CreateSecondaryNetwork/DeleteSecondaryNetwork/
// DescribeSecondaryNetworks, CreateSecondarySubnet/DeleteSecondarySubnet/
// DescribeSecondarySubnets, DescribeSecondaryInterfaces) plus the read-only
// Outpost LAG / Service Link Virtual Interface family (DescribeOutpostLags,
// DescribeServiceLinkVirtualInterfaces). Secondary interfaces, Outpost LAGs, and
// service link virtual interfaces have no Create API in EC2 -- they are
// physical/attached resources that only support Describe, so (mirroring
// backend_local_gateway.go's LocalGatewayVirtualInterface precedent) they are
// exposed via Seed* methods for tests and start out empty.

var (
	// ErrSecondaryNetworkNotFound is returned when a secondary network ID does not exist.
	ErrSecondaryNetworkNotFound = errors.New("InvalidSecondaryNetworkID.NotFound")

	// ErrSecondarySubnetNotFound is returned when a secondary subnet ID does not exist.
	ErrSecondarySubnetNotFound = errors.New("InvalidSecondarySubnetID.NotFound")

	// ErrSecondaryNetworkHasSubnets is returned when DeleteSecondaryNetwork is
	// called on a secondary network that still has secondary subnets.
	ErrSecondaryNetworkHasSubnets = errors.New("DependencyViolation")
)

const (
	secondaryNetworkTypeRdma = "rdma"

	secondaryStateCreateComplete = "create-complete"
	secondaryStateDeleteComplete = "delete-complete"

	secondaryCidrStateAssociated = "associated"
)

// SecondaryNetworkCidrAssoc represents an IPv4 CIDR block association on a
// SecondaryNetwork.
type SecondaryNetworkCidrAssoc struct {
	AssociationID string `json:"associationId,omitempty"`
	CidrBlock     string `json:"cidrBlock,omitempty"`
	State         string `json:"state,omitempty"`
}

// SecondaryNetwork represents an EC2 secondary network (e.g. an RDMA network
// used by EFA-capable instance types).
type SecondaryNetwork struct {
	Tags                      map[string]string           `json:"tags,omitempty"`
	SecondaryNetworkID        string                      `json:"secondaryNetworkId,omitempty"`
	SecondaryNetworkArn       string                      `json:"secondaryNetworkArn,omitempty"`
	OwnerID                   string                      `json:"ownerId,omitempty"`
	Type                      string                      `json:"type,omitempty"`
	State                     string                      `json:"state,omitempty"`
	Ipv4CidrBlockAssociations []SecondaryNetworkCidrAssoc `json:"ipv4CidrBlockAssociations,omitempty"`
}

// SecondarySubnetCidrAssoc represents an IPv4 CIDR block association on a
// SecondarySubnet.
type SecondarySubnetCidrAssoc struct {
	AssociationID string `json:"associationId,omitempty"`
	CidrBlock     string `json:"cidrBlock,omitempty"`
	State         string `json:"state,omitempty"`
}

// SecondarySubnet represents an EC2 secondary subnet within a SecondaryNetwork.
type SecondarySubnet struct {
	Tags                      map[string]string          `json:"tags,omitempty"`
	SecondarySubnetID         string                     `json:"secondarySubnetId,omitempty"`
	SecondarySubnetArn        string                     `json:"secondarySubnetArn,omitempty"`
	SecondaryNetworkID        string                     `json:"secondaryNetworkId,omitempty"`
	SecondaryNetworkType      string                     `json:"secondaryNetworkType,omitempty"`
	OwnerID                   string                     `json:"ownerId,omitempty"`
	AvailabilityZone          string                     `json:"availabilityZone,omitempty"`
	AvailabilityZoneID        string                     `json:"availabilityZoneId,omitempty"`
	State                     string                     `json:"state,omitempty"`
	Ipv4CidrBlockAssociations []SecondarySubnetCidrAssoc `json:"ipv4CidrBlockAssociations,omitempty"`
}

// SecondaryInterface represents an EC2 secondary network interface attached to
// an RDMA-capable instance. Secondary interfaces have no Create API in EC2 --
// they are exposed for Describe only and populated via SeedSecondaryInterface.
type SecondaryInterface struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	OwnerID                string            `json:"ownerId,omitempty"`
	AvailabilityZoneID     string            `json:"availabilityZoneId,omitempty"`
	SecondaryNetworkID     string            `json:"secondaryNetworkId,omitempty"`
	SecondaryNetworkType   string            `json:"secondaryNetworkType,omitempty"`
	SecondarySubnetID      string            `json:"secondarySubnetId,omitempty"`
	SecondaryInterfaceID   string            `json:"secondaryInterfaceId,omitempty"`
	AvailabilityZone       string            `json:"availabilityZone,omitempty"`
	SecondaryInterfaceType string            `json:"secondaryInterfaceType,omitempty"`
	MacAddress             string            `json:"macAddress,omitempty"`
	Status                 string            `json:"status,omitempty"`
	SecondaryInterfaceArn  string            `json:"secondaryInterfaceArn,omitempty"`
	InstanceID             string            `json:"instanceId,omitempty"`
	PrivateIpv4Addresses   []string          `json:"privateIpv4Addresses,omitempty"`
	SourceDestCheck        bool              `json:"sourceDestCheck,omitempty"`
}

// ServiceLinkVirtualInterface represents an Outpost service link virtual
// interface. Like LocalGatewayVirtualInterface, these are Outpost-provisioned
// hardware resources with no Create API in EC2; SeedServiceLinkVirtualInterface
// injects them for tests.
type ServiceLinkVirtualInterface struct {
	Tags                           map[string]string `json:"tags,omitempty"`
	ServiceLinkVirtualInterfaceID  string            `json:"serviceLinkVirtualInterfaceId,omitempty"`
	ServiceLinkVirtualInterfaceArn string            `json:"serviceLinkVirtualInterfaceArn,omitempty"`
	OutpostID                      string            `json:"outpostId,omitempty"`
	OutpostArn                     string            `json:"outpostArn,omitempty"`
	OutpostLagID                   string            `json:"outpostLagId,omitempty"`
	OwnerID                        string            `json:"ownerId,omitempty"`
	LocalAddress                   string            `json:"localAddress,omitempty"`
	PeerAddress                    string            `json:"peerAddress,omitempty"`
	ConfigurationState             string            `json:"configurationState,omitempty"`
	PeerBgpAsn                     int64             `json:"peerBgpAsn,omitempty"`
	Vlan                           int32             `json:"vlan,omitempty"`
}

// OutpostLag represents an Outpost link aggregation group (LAG). Like
// LocalGatewayVirtualInterface, this is an Outpost-provisioned hardware
// resource with no Create API in EC2; SeedOutpostLag injects it for tests.
type OutpostLag struct {
	Tags                            map[string]string `json:"tags,omitempty"`
	OutpostLagID                    string            `json:"outpostLagId,omitempty"`
	OutpostArn                      string            `json:"outpostArn,omitempty"`
	OwnerID                         string            `json:"ownerId,omitempty"`
	State                           string            `json:"state,omitempty"`
	LocalGatewayVirtualInterfaceIDs []string          `json:"localGatewayVirtualInterfaceIds,omitempty"`
	ServiceLinkVirtualInterfaceIDs  []string          `json:"serviceLinkVirtualInterfaceIds,omitempty"`
}

// resetSecondaryNetworkMapsLocked re-initialises all maps owned by this file.
// Must be called with b.mu held.
func (b *InMemoryBackend) resetSecondaryNetworkMapsLocked() {
}

// ---- Secondary Networks ----

// CreateSecondaryNetwork creates a new secondary network with the given IPv4
// CIDR block and type ("rdma").
func (b *InMemoryBackend) CreateSecondaryNetwork(
	ipv4CidrBlock, networkType string, tags map[string]string,
) (*SecondaryNetwork, error) {
	if ipv4CidrBlock == "" {
		return nil, fmt.Errorf("%w: Ipv4CidrBlock is required", ErrInvalidParameter)
	}

	if networkType == "" {
		networkType = secondaryNetworkTypeRdma
	}

	b.mu.Lock("CreateSecondaryNetwork")
	defer b.mu.Unlock()

	id := "secnet-" + uuid.New().String()[:17]
	net := &SecondaryNetwork{
		SecondaryNetworkID:  id,
		SecondaryNetworkArn: fmt.Sprintf("arn:aws:ec2:%s:%s:secondary-network/%s", b.Region, b.AccountID, id),
		OwnerID:             b.AccountID,
		Type:                networkType,
		State:               secondaryStateCreateComplete,
		Ipv4CidrBlockAssociations: []SecondaryNetworkCidrAssoc{
			{
				AssociationID: "secnet-cidr-" + uuid.New().String()[:8],
				CidrBlock:     ipv4CidrBlock,
				State:         secondaryCidrStateAssociated,
			},
		},
		Tags: tags,
	}
	b.secondaryNetworks.Put(net)

	cp := *net

	return &cp, nil
}

// DeleteSecondaryNetwork deletes a secondary network. Fails with
// ErrSecondaryNetworkHasSubnets if any secondary subnets still reference it.
func (b *InMemoryBackend) DeleteSecondaryNetwork(id string) (*SecondaryNetwork, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: SecondaryNetworkId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSecondaryNetwork")
	defer b.mu.Unlock()

	net, ok := b.secondaryNetworks.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSecondaryNetworkNotFound, id)
	}

	for _, s := range b.secondarySubnets.All() {
		if s.SecondaryNetworkID == id {
			return nil, fmt.Errorf(
				"%w: secondary network %s still has secondary subnets", ErrSecondaryNetworkHasSubnets, id,
			)
		}
	}
	b.secondaryNetworks.Delete(id)

	cp := *net
	cp.State = secondaryStateDeleteComplete

	return &cp, nil
}

// DescribeSecondaryNetworks returns secondary networks, optionally filtered by ID.
func (b *InMemoryBackend) DescribeSecondaryNetworks(ids []string) []*SecondaryNetwork {
	b.mu.RLock("DescribeSecondaryNetworks")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*SecondaryNetwork, 0, b.secondaryNetworks.Len())

	for _, n := range b.secondaryNetworks.All() {
		if len(idSet) > 0 && !idSet[n.SecondaryNetworkID] {
			continue
		}

		cp := *n
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SecondaryNetworkID < out[j].SecondaryNetworkID })

	return out
}

// ---- Secondary Subnets ----

// CreateSecondarySubnet creates a new secondary subnet within a secondary network.
func (b *InMemoryBackend) CreateSecondarySubnet(
	ipv4CidrBlock, secondaryNetworkID, availabilityZone, availabilityZoneID string, tags map[string]string,
) (*SecondarySubnet, error) {
	if ipv4CidrBlock == "" {
		return nil, fmt.Errorf("%w: Ipv4CidrBlock is required", ErrInvalidParameter)
	}

	if secondaryNetworkID == "" {
		return nil, fmt.Errorf("%w: SecondaryNetworkId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSecondarySubnet")
	defer b.mu.Unlock()

	net, ok := b.secondaryNetworks.Get(secondaryNetworkID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSecondaryNetworkNotFound, secondaryNetworkID)
	}

	if availabilityZone == "" && availabilityZoneID == "" {
		availabilityZone = b.Region + "a"
	}

	id := "secsubnet-" + uuid.New().String()[:17]
	sub := &SecondarySubnet{
		SecondarySubnetID:    id,
		SecondarySubnetArn:   fmt.Sprintf("arn:aws:ec2:%s:%s:secondary-subnet/%s", b.Region, b.AccountID, id),
		SecondaryNetworkID:   secondaryNetworkID,
		SecondaryNetworkType: net.Type,
		OwnerID:              b.AccountID,
		AvailabilityZone:     availabilityZone,
		AvailabilityZoneID:   availabilityZoneID,
		State:                secondaryStateCreateComplete,
		Ipv4CidrBlockAssociations: []SecondarySubnetCidrAssoc{
			{
				AssociationID: "secsubnet-cidr-" + uuid.New().String()[:8],
				CidrBlock:     ipv4CidrBlock,
				State:         secondaryCidrStateAssociated,
			},
		},
		Tags: tags,
	}
	b.secondarySubnets.Put(sub)

	cp := *sub

	return &cp, nil
}

// DeleteSecondarySubnet deletes a secondary subnet.
func (b *InMemoryBackend) DeleteSecondarySubnet(id string) (*SecondarySubnet, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: SecondarySubnetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSecondarySubnet")
	defer b.mu.Unlock()

	sub, ok := b.secondarySubnets.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSecondarySubnetNotFound, id)
	}
	b.secondarySubnets.Delete(id)

	cp := *sub
	cp.State = secondaryStateDeleteComplete

	return &cp, nil
}

// DescribeSecondarySubnets returns secondary subnets, optionally filtered by ID.
func (b *InMemoryBackend) DescribeSecondarySubnets(ids []string) []*SecondarySubnet {
	b.mu.RLock("DescribeSecondarySubnets")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*SecondarySubnet, 0, b.secondarySubnets.Len())

	for _, s := range b.secondarySubnets.All() {
		if len(idSet) > 0 && !idSet[s.SecondarySubnetID] {
			continue
		}

		cp := *s
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SecondarySubnetID < out[j].SecondarySubnetID })

	return out
}

// ---- Secondary Interfaces (Describe-only; seeded for tests) ----

// SeedSecondaryInterface injects a secondary interface into the backend for
// testing. Secondary interfaces are created implicitly when an RDMA-capable
// instance is launched into a secondary subnet; EC2 exposes no Create API for
// them directly, so this emulator seeds them the same way it seeds
// LocalGatewayVirtualInterface. The value is deep-copied; a generated ID is
// assigned if si.SecondaryInterfaceID is empty.
func (b *InMemoryBackend) SeedSecondaryInterface(si SecondaryInterface) (*SecondaryInterface, error) {
	b.mu.Lock("SeedSecondaryInterface")
	defer b.mu.Unlock()

	if si.SecondaryInterfaceID == "" {
		si.SecondaryInterfaceID = "secni-" + uuid.New().String()[:17]
	}

	if si.OwnerID == "" {
		si.OwnerID = b.AccountID
	}

	cp := si
	b.secondaryInterfaces.Put(&cp)

	out := cp

	return &out, nil
}

// DescribeSecondaryInterfaces returns secondary interfaces, optionally
// filtered by ID.
func (b *InMemoryBackend) DescribeSecondaryInterfaces(ids []string) []*SecondaryInterface {
	b.mu.RLock("DescribeSecondaryInterfaces")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*SecondaryInterface, 0, b.secondaryInterfaces.Len())

	for _, si := range b.secondaryInterfaces.All() {
		if len(idSet) > 0 && !idSet[si.SecondaryInterfaceID] {
			continue
		}

		cp := *si
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SecondaryInterfaceID < out[j].SecondaryInterfaceID })

	return out
}

// ---- Outpost LAGs / Service Link Virtual Interfaces (Describe-only; seeded) ----

// SeedServiceLinkVirtualInterface injects a service link virtual interface
// into the backend for testing. Like LocalGatewayVirtualInterface, these are
// Outpost-provisioned hardware resources with no Create API in EC2. The value
// is deep-copied; a generated ID is assigned if empty.
func (b *InMemoryBackend) SeedServiceLinkVirtualInterface(
	vif ServiceLinkVirtualInterface,
) (*ServiceLinkVirtualInterface, error) {
	b.mu.Lock("SeedServiceLinkVirtualInterface")
	defer b.mu.Unlock()

	if vif.ServiceLinkVirtualInterfaceID == "" {
		vif.ServiceLinkVirtualInterfaceID = "svif-" + uuid.New().String()[:17]
	}

	if vif.OwnerID == "" {
		vif.OwnerID = b.AccountID
	}

	if vif.ConfigurationState == "" {
		vif.ConfigurationState = stateAvailable
	}

	cp := vif
	b.serviceLinkVirtualInterfaces.Put(&cp)

	out := cp

	return &out, nil
}

// DescribeServiceLinkVirtualInterfaces returns service link virtual
// interfaces, optionally filtered by ID.
func (b *InMemoryBackend) DescribeServiceLinkVirtualInterfaces(ids []string) []*ServiceLinkVirtualInterface {
	b.mu.RLock("DescribeServiceLinkVirtualInterfaces")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*ServiceLinkVirtualInterface, 0, b.serviceLinkVirtualInterfaces.Len())

	for _, vif := range b.serviceLinkVirtualInterfaces.All() {
		if len(idSet) > 0 && !idSet[vif.ServiceLinkVirtualInterfaceID] {
			continue
		}

		cp := *vif
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ServiceLinkVirtualInterfaceID < out[j].ServiceLinkVirtualInterfaceID
	})

	return out
}

// SeedOutpostLag injects an Outpost LAG into the backend for testing. Like
// LocalGatewayVirtualInterface, this is an Outpost-provisioned hardware
// resource with no Create API in EC2. The value is deep-copied; a generated ID
// is assigned if empty.
func (b *InMemoryBackend) SeedOutpostLag(lag OutpostLag) (*OutpostLag, error) {
	b.mu.Lock("SeedOutpostLag")
	defer b.mu.Unlock()

	if lag.OutpostLagID == "" {
		lag.OutpostLagID = "lag-" + uuid.New().String()[:17]
	}

	if lag.OwnerID == "" {
		lag.OwnerID = b.AccountID
	}

	if lag.State == "" {
		lag.State = stateAvailable
	}

	cp := lag
	b.outpostLags.Put(&cp)

	out := cp

	return &out, nil
}

// DescribeOutpostLags returns Outpost LAGs, optionally filtered by ID.
func (b *InMemoryBackend) DescribeOutpostLags(ids []string) []*OutpostLag {
	b.mu.RLock("DescribeOutpostLags")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*OutpostLag, 0, b.outpostLags.Len())

	for _, lag := range b.outpostLags.All() {
		if len(idSet) > 0 && !idSet[lag.OutpostLagID] {
			continue
		}

		cp := *lag
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].OutpostLagID < out[j].OutpostLagID })

	return out
}
