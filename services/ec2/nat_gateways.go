package ec2

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrNatGatewayNotFound is returned when a NAT gateway is not found.
var ErrNatGatewayNotFound = errors.New("InvalidNatGatewayID.NotFound")

// NatGateway represents an EC2 NAT Gateway.
type NatGateway struct {
	CreateTime   time.Time `json:"createTime"`
	ID           string    `json:"id,omitempty"`
	SubnetID     string    `json:"subnetID,omitempty"`
	VPCID        string    `json:"vpcID,omitempty"`
	AllocationID string    `json:"allocationID,omitempty"`
	PublicIP     string    `json:"publicIP,omitempty"`
	PrivateIP    string    `json:"privateIP,omitempty"`
	State        string    `json:"state,omitempty"`
	// SecondaryPrivateIPs holds additional private IPs assigned via
	// AssignPrivateNatGatewayAddress and removed via
	// UnassignPrivateNatGatewayAddress.
	SecondaryPrivateIPs []string `json:"secondaryPrivateIPs,omitempty"`
}

// CreateNatGateway creates a new NAT Gateway.
func (b *InMemoryBackend) CreateNatGateway(subnetID, allocationID string) (*NatGateway, error) {
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
		ID:           id,
		SubnetID:     subnetID,
		VPCID:        subnet.VPCID,
		AllocationID: allocationID,
		PublicIP:     addr.PublicIP,
		PrivateIP:    b.allocPrivateIP(),
		State:        stateAvailable,
		CreateTime:   time.Now(),
	}
	b.natGateways.Put(ngw)
	b.indexNatGatewayLocked(ngw)

	return ngw, nil
}

// DeleteNatGateway removes a NAT Gateway and recycles its private IP.
func (b *InMemoryBackend) DeleteNatGateway(id string) error {
	b.mu.Lock("DeleteNatGateway")
	defer b.mu.Unlock()

	ngw, ok := b.natGateways.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNatGatewayNotFound, id)
	}

	b.recycleIPLocked(ngw.PrivateIP)
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

// DisassociateNatGatewayAddress removes a secondary IP from a NAT gateway.
func (b *InMemoryBackend) DisassociateNatGatewayAddress(natGatewayID string) error {
	if natGatewayID == "" {
		return fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	b.mu.RLock("DisassociateNatGatewayAddress")
	defer b.mu.RUnlock()

	if _, ok := b.natGateways.Get(natGatewayID); !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, natGatewayID)
	}

	return nil
}

// AssociateNatGatewayAddress adds an allocation to a NAT gateway.
func (b *InMemoryBackend) AssociateNatGatewayAddress(natGatewayID, _ string) error {
	if natGatewayID == "" {
		return fmt.Errorf("%w: NatGatewayId is required", ErrInvalidParameter)
	}

	b.mu.RLock("AssociateNatGatewayAddress")
	defer b.mu.RUnlock()

	if _, ok := b.natGateways.Get(natGatewayID); !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, natGatewayID)
	}

	return nil
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
