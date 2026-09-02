package workmail

import (
	"fmt"
)

// --- Inbound DMARC Settings ---

// PutInboundDmarcSettings sets inbound DMARC enforcement for an org.
func (b *InMemoryBackend) PutInboundDmarcSettings(orgID string, enforced bool) error {
	b.mu.Lock("PutInboundDmarcSettings")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	b.inboundDmarc[orgID] = enforced

	return nil
}

// DescribeInboundDmarcSettings returns whether inbound DMARC is enforced for an org.
func (b *InMemoryBackend) DescribeInboundDmarcSettings(orgID string) (bool, error) {
	b.mu.RLock("DescribeInboundDmarcSettings")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return false, fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}

	return b.inboundDmarc[orgID], nil
}
