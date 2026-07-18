package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// ---- VPN Gateways ----

// CreateVpnGateway creates a new virtual private gateway.
func (b *InMemoryBackend) CreateVpnGateway(gatewayType string) (*VpnGateway, error) {
	if gatewayType == "" {
		gatewayType = vpnTypeIPSec
	}

	b.mu.Lock("CreateVpnGateway")
	defer b.mu.Unlock()

	vgw := &VpnGateway{
		VpnGatewayID: "vgw-" + uuid.New().String()[:8],
		State:        stateAvailable,
		Type:         gatewayType,
	}
	b.vpnGateways.Put(vgw)

	cp := *vgw

	return &cp, nil
}

// DescribeVpnGateways returns virtual private gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpnGateways(ids []string) []*VpnGateway {
	b.mu.RLock("DescribeVpnGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*VpnGateway, 0, b.vpnGateways.Len())

	for _, vgw := range b.vpnGateways.All() {
		if len(idSet) > 0 && !idSet[vgw.VpnGatewayID] {
			continue
		}

		cp := *vgw
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].VpnGatewayID < out[j].VpnGatewayID
	})

	return out
}

// DeleteVpnGateway removes a VPN gateway.
func (b *InMemoryBackend) DeleteVpnGateway(id string) error {
	if id == "" {
		return fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteVpnGateway")
	defer b.mu.Unlock()

	if _, ok := b.vpnGateways.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, id)
	}
	b.vpnGateways.Delete(id)

	return nil
}

// AttachVpnGateway attaches a VPN gateway to a VPC.
func (b *InMemoryBackend) AttachVpnGateway(vgwID, vpcID string) error {
	if vgwID == "" {
		return fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	if vpcID == "" {
		return fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AttachVpnGateway")
	defer b.mu.Unlock()

	vgw, ok := b.vpnGateways.Get(vgwID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vgwID)
	}

	if _, exists := b.vpcs.Get(vpcID); !exists {
		return fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	vgw.AttachedVPCID = vpcID
	vgw.AttachmentState = attachmentStateAttached

	return nil
}

// DetachVpnGateway detaches a VPN gateway from a VPC.
func (b *InMemoryBackend) DetachVpnGateway(vgwID, vpcID string) error {
	if vgwID == "" {
		return fmt.Errorf("%w: VpnGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DetachVpnGateway")
	defer b.mu.Unlock()

	vgw, ok := b.vpnGateways.Get(vgwID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrVpnGatewayNotFound, vgwID)
	}

	_ = vpcID
	vgw.AttachedVPCID = ""
	vgw.AttachmentState = attachmentStateDetached

	return nil
}

// ---- Customer Gateways ----

// CreateCustomerGateway creates a new customer gateway.
func (b *InMemoryBackend) CreateCustomerGateway(
	gatewayType, ipAddress, bgpAsn string,
) (*CustomerGateway, error) {
	if ipAddress == "" {
		return nil, fmt.Errorf("%w: IpAddress is required", ErrInvalidParameter)
	}

	if gatewayType == "" {
		gatewayType = vpnTypeIPSec
	}

	if bgpAsn == "" {
		bgpAsn = "65000"
	}

	b.mu.Lock("CreateCustomerGateway")
	defer b.mu.Unlock()

	cgw := &CustomerGateway{
		CustomerGatewayID: "cgw-" + uuid.New().String()[:8],
		State:             stateAvailable,
		Type:              gatewayType,
		BgpAsn:            bgpAsn,
		IPAddress:         ipAddress,
	}
	b.customerGateways.Put(cgw)

	cp := *cgw

	return &cp, nil
}

// DescribeCustomerGateways returns customer gateways, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeCustomerGateways(ids []string) []*CustomerGateway {
	b.mu.RLock("DescribeCustomerGateways")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*CustomerGateway, 0, b.customerGateways.Len())

	for _, cgw := range b.customerGateways.All() {
		if len(idSet) > 0 && !idSet[cgw.CustomerGatewayID] {
			continue
		}

		cp := *cgw
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CustomerGatewayID < out[j].CustomerGatewayID
	})

	return out
}

// DeleteCustomerGateway removes a customer gateway.
func (b *InMemoryBackend) DeleteCustomerGateway(id string) error {
	if id == "" {
		return fmt.Errorf("%w: CustomerGatewayId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCustomerGateway")
	defer b.mu.Unlock()

	if _, ok := b.customerGateways.Get(id); !ok {
		return fmt.Errorf("%w: %s", ErrCustomerGatewayNotFound, id)
	}
	b.customerGateways.Delete(id)

	return nil
}
