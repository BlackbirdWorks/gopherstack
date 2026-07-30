package ec2

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrNatGatewayNotFound is returned when a NAT gateway is not found.
var ErrNatGatewayNotFound = errors.New("InvalidNatGatewayID.NotFound")

// natGatewayConnectivityTypePublic is the only ConnectivityType this backend
// creates: CreateNatGateway always requires an AllocationId (an Elastic IP),
// which is the defining trait of a public NAT gateway in real AWS. Private
// NAT gateways (no AllocationId, ConnectivityType=private) are not modeled —
// see PARITY.md.
const natGatewayConnectivityTypePublic = "public"

// NatGateway represents an EC2 NAT Gateway.
type NatGateway struct {
	CreateTime time.Time `json:"createTime"`
	ID         string    `json:"id,omitempty"`
	SubnetID   string    `json:"subnetID,omitempty"`
	VPCID      string    `json:"vpcID,omitempty"`
	// AvailabilityZone is the AZ of the gateway's subnet, matching real AWS's
	// NatGatewayAddress.AvailabilityZone for the primary (and, in this
	// single-AZ-only mock, every secondary) address.
	AvailabilityZone string `json:"availabilityZone,omitempty"`
	// AllocationID / AssociationID / PublicIP / PrivateIP describe the
	// gateway's primary (IsPrimary=true) EIP association, set at creation.
	AllocationID     string `json:"allocationID,omitempty"`
	AssociationID    string `json:"associationID,omitempty"`
	PublicIP         string `json:"publicIP,omitempty"`
	PrivateIP        string `json:"privateIP,omitempty"`
	State            string `json:"state,omitempty"`
	ConnectivityType string `json:"connectivityType,omitempty"`
	// SecondaryAddresses holds additional public IP associations added via
	// AssociateNatGatewayAddress and removed via DisassociateNatGatewayAddress.
	SecondaryAddresses []NatGatewayAddress `json:"secondaryAddresses,omitempty"`
	// SecondaryPrivateIPs holds additional private IPs assigned via
	// AssignPrivateNatGatewayAddress and removed via
	// UnassignPrivateNatGatewayAddress.
	SecondaryPrivateIPs []string `json:"secondaryPrivateIPs,omitempty"`
}

// NatGatewayAddress represents one secondary (non-primary) EIP association on
// a public NAT gateway, as added by AssociateNatGatewayAddress.
type NatGatewayAddress struct {
	AllocationID  string `json:"allocationID,omitempty"`
	AssociationID string `json:"associationID,omitempty"`
	PrivateIP     string `json:"privateIP,omitempty"`
	PublicIP      string `json:"publicIP,omitempty"`
}

// CreateNatGateway creates a new NAT Gateway.
func (b *InMemoryBackend) CreateNatGateway(
	subnetID, allocationID string, tags map[string]string,
) (*NatGateway, error) {
	b.mu.Lock("CreateNatGateway")
	defer b.mu.Unlock()

	subnet, ok := b.subnets.Get(subnetID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
	}

	addr, ok := b.addresses.Get(allocationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAddressNotFound, allocationID)
	}

	id := "nat-" + uuid.New().String()[:17]
	ngw := &NatGateway{
		ID:               id,
		SubnetID:         subnetID,
		VPCID:            subnet.VPCID,
		AvailabilityZone: subnet.AvailabilityZone,
		AllocationID:     allocationID,
		AssociationID:    "eipassoc-" + uuid.New().String()[:17],
		PublicIP:         addr.PublicIP,
		PrivateIP:        b.allocPrivateIP(),
		State:            stateAvailable,
		ConnectivityType: natGatewayConnectivityTypePublic,
		CreateTime:       time.Now(),
	}
	b.natGateways.Put(ngw)
	b.indexNatGatewayLocked(ngw)
	b.setTagsLocked(id, tags)

	return ngw, nil
}

// DeleteNatGateway removes a NAT Gateway and recycles every private IP it
// holds: the primary address, any secondary EIP associations, and any
// secondary private IPs assigned via AssignPrivateNatGatewayAddress.
func (b *InMemoryBackend) DeleteNatGateway(id string) error {
	b.mu.Lock("DeleteNatGateway")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNatGatewayNotFound, id)
	}

	b.recycleIPLocked(ngw.PrivateIP)

	for _, sa := range ngw.SecondaryAddresses {
		b.recycleIPLocked(sa.PrivateIP)
	}

	for _, ip := range ngw.SecondaryPrivateIPs {
		b.recycleIPLocked(ip)
	}

	b.deindexNatGatewayLocked(ngw)
	b.natGateways.Delete(id)
	delete(b.tags, id)

	return nil
}

// DescribeNatGateways returns NAT Gateways, optionally filtered by IDs.
// When ids are provided, lookups are O(len(ids)) via the NAT-gateway map
// rather than scanning every gateway in the backend.
func (b *InMemoryBackend) DescribeNatGateways(ids []string) []*NatGateway {
	b.mu.RLock("DescribeNatGateways")
	defer b.mu.RUnlock()

	if len(ids) > 0 {
		out := make([]*NatGateway, 0, len(ids))

		for _, id := range ids {
			ngw, ok := b.natGateways.Get(id)
			if !ok {
				continue
			}

			cp := *ngw
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*NatGateway, 0, b.natGateways.Len())

	for _, ngw := range b.natGateways.All() {
		cp := *ngw
		out = append(out, &cp)
	}

	return out
}

// DisassociateNatGatewayAddress removes secondary EIP associations (added via
// AssociateNatGatewayAddress) from a public NAT gateway by AssociationId,
// recycling their private IPs. Returns the updated NAT gateway.
func (b *InMemoryBackend) DisassociateNatGatewayAddress(
	natGatewayID string, associationIDs []string,
) (*NatGateway, error) {
	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if len(associationIDs) == 0 {
		return nil, fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNatGatewayNotFound, natGatewayID)
	}

	for _, assocID := range associationIDs {
		idx := -1

		for i, sa := range ngw.SecondaryAddresses {
			if sa.AssociationID == assocID {
				idx = i

				break
			}
		}

		if idx == -1 {
			return nil, fmt.Errorf("%w: %s", ErrAssociationNotFound, assocID)
		}

		b.recycleIPLocked(ngw.SecondaryAddresses[idx].PrivateIP)
		ngw.SecondaryAddresses = append(
			ngw.SecondaryAddresses[:idx], ngw.SecondaryAddresses[idx+1:]...,
		)
	}

	cp := *ngw
	cp.SecondaryAddresses = append([]NatGatewayAddress(nil), ngw.SecondaryAddresses...)

	return &cp, nil
}

// AssociateNatGatewayAddress associates one or more additional Elastic IP
// allocations with a public NAT gateway, allocating a fresh private IP and
// AssociationId for each. Returns the updated NAT gateway.
func (b *InMemoryBackend) AssociateNatGatewayAddress(
	natGatewayID string, allocationIDs []string,
) (*NatGateway, error) {
	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if len(allocationIDs) == 0 {
		return nil, fmt.Errorf("%w: AllocationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNatGatewayNotFound, natGatewayID)
	}

	for _, allocID := range allocationIDs {
		addr, addrOK := b.addresses.Get(allocID)
		if !addrOK {
			return nil, fmt.Errorf("%w: %s", ErrAddressNotFound, allocID)
		}

		ngw.SecondaryAddresses = append(ngw.SecondaryAddresses, NatGatewayAddress{
			AllocationID:  allocID,
			AssociationID: "eipassoc-" + uuid.New().String()[:17],
			PrivateIP:     b.allocPrivateIP(),
			PublicIP:      addr.PublicIP,
		})
	}

	cp := *ngw
	cp.SecondaryAddresses = append([]NatGatewayAddress(nil), ngw.SecondaryAddresses...)

	return &cp, nil
}

// AssignPrivateNatGatewayAddress assigns a new secondary private IP to a NAT
// gateway, appending it to the gateway's real address state. See
// UnassignPrivateNatGatewayAddress (nat_gateways.go) for the inverse.
func (b *InMemoryBackend) AssignPrivateNatGatewayAddress(natGatewayID string) error {
	if natGatewayID == "" {
		return fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AssignPrivateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, natGatewayID)
	}

	ngw.SecondaryPrivateIPs = append(ngw.SecondaryPrivateIPs, b.allocPrivateIP())

	return nil
}

// ---- NAT gateway address management ----

// UnassignPrivateNatGatewayAddress removes previously assigned secondary
// private IPs from a NAT gateway, mutating the existing NAT gateway's address
// state.
func (b *InMemoryBackend) UnassignPrivateNatGatewayAddress(
	natGatewayID string, privateIPs []string,
) (*NatGateway, error) {
	if natGatewayID == "" {
		return nil, fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	if len(privateIPs) == 0 {
		return nil, fmt.Errorf("%w: PrivateIpAddress is required", ErrInvalidParameter)
	}

	b.mu.Lock("UnassignPrivateNatGatewayAddress")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(natGatewayID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNatGatewayNotFound, natGatewayID)
	}

	remove := make(map[string]bool, len(privateIPs))
	for _, ip := range privateIPs {
		remove[ip] = true
	}

	kept := ngw.SecondaryPrivateIPs[:0:0]

	for _, ip := range ngw.SecondaryPrivateIPs {
		if !remove[ip] {
			kept = append(kept, ip)
		}
	}

	ngw.SecondaryPrivateIPs = kept

	cp := *ngw

	return &cp, nil
}
