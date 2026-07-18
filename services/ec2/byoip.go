package ec2

import "fmt"

func (b *InMemoryBackend) ProvisionByoipCidr(cidr, description string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("ProvisionByoipCidr")
	defer b.mu.Unlock()

	entry := &ByoipCidr{
		Cidr:          cidr,
		State:         "pending-provision",
		StatusMessage: description,
	}
	b.byoipCidrs.Put(entry)

	cp := *entry

	return &cp, nil
}

func (b *InMemoryBackend) DeprovisionByoipCidr(cidr string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeprovisionByoipCidr")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs.Get(cidr)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, cidr)
	}

	entry.State = "pending-deprovision"
	b.byoipCidrs.Delete(cidr)

	cp := *entry

	return &cp, nil
}

func (b *InMemoryBackend) WithdrawByoipCidr(cidr string) (*ByoipCidr, error) {
	if cidr == "" {
		return nil, fmt.Errorf("%w: Cidr is required", ErrInvalidParameter)
	}

	b.mu.Lock("WithdrawByoipCidr")
	defer b.mu.Unlock()

	entry, ok := b.byoipCidrs.Get(cidr)
	if !ok {
		entry = &ByoipCidr{Cidr: cidr}
		b.byoipCidrs.Put(entry)
	}

	entry.State = stateByoipAdvertised

	cp := *entry

	return &cp, nil
}

// ---- Carrier Gateways backend methods ----
