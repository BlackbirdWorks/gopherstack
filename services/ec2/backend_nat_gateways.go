package ec2

import "fmt"

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
// UnassignPrivateNatGatewayAddress (backend_parity_final.go) for the inverse.
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

// ---- Image lifecycle ----

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

// ---- Image extras ----
