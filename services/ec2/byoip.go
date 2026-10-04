package ec2

import (
	"fmt"
	"net/netip"
)

// stateByoipNotPubliclyAdvertisable is the state an IPv6 range provisioned
// with PubliclyAdvertisable=false reports.
const stateByoipNotPubliclyAdvertisable = "provisioned-not-publicly-advertisable"

// ProvisionByoipCidr provisions a BYOIP range; publiclyAdvertisable=false on an
// IPv6 range leaves it provisioned-not-publicly-advertisable.
func (b *InMemoryBackend) ProvisionByoipCidr(
	cidr, description string, publiclyAdvertisable *bool,
) (*ByoipCidr, error) {
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
	if publiclyAdvertisable != nil && !*publiclyAdvertisable && isIPv6CIDR(cidr) {
		entry.State = stateByoipNotPubliclyAdvertisable
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

func isIPv6CIDR(cidr string) bool {
	p, err := netip.ParsePrefix(cidr)

	return err == nil && p.Addr().Is6()
}
