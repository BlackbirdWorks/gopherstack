package apprunner

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// AssociateCustomDomain associates a custom domain with a service.
func (b *InMemoryBackend) AssociateCustomDomain(
	serviceArn, domainName string,
	enableWWW bool,
) (*CustomDomain, error) {
	b.mu.Lock("AssociateCustomDomain")
	defer b.mu.Unlock()

	if !b.services.Has(serviceArn) {
		// Unlike Describe/Delete/Disassociate*, AssociateCustomDomain's
		// documented error set has no ResourceNotFoundException (only
		// InternalServiceErrorException, InvalidRequestException, and
		// InvalidStateException), so an unknown ServiceArn is wrapped as
		// ErrInvalidParameter here rather than the usual ErrNotFound.
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrInvalidParameter)
	}

	for _, d := range b.customDomains[serviceArn] {
		if d.DomainName == domainName {
			return nil, fmt.Errorf("domain %s already associated: %w", domainName, ErrAlreadyExists)
		}
	}

	cd := &storedCustomDomain{
		DomainName:         domainName,
		Status:             customDomainStatusActive,
		EnableWWWSubdomain: enableWWW,
	}
	b.customDomains[serviceArn] = append(b.customDomains[serviceArn], cd)

	cp := cd.toCustomDomain()

	return &cp, nil
}

// DisassociateCustomDomain removes a custom domain from a service.
func (b *InMemoryBackend) DisassociateCustomDomain(serviceArn, domainName string) (*CustomDomain, error) {
	b.mu.Lock("DisassociateCustomDomain")
	defer b.mu.Unlock()

	if !b.services.Has(serviceArn) {
		return nil, fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	domains := b.customDomains[serviceArn]
	for i, d := range domains {
		if d.DomainName == domainName {
			cp := d.toCustomDomain()
			b.customDomains[serviceArn] = append(domains[:i], domains[i+1:]...)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("domain %s not found on service %s: %w", domainName, serviceArn, ErrNotFound)
}

// DescribeCustomDomains returns custom domains for a service with pagination.
func (b *InMemoryBackend) DescribeCustomDomains(
	serviceArn string,
	maxResults int32,
	nextToken string,
) ([]*CustomDomain, string, string, error) {
	b.mu.RLock("DescribeCustomDomains")
	defer b.mu.RUnlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return nil, "", "", fmt.Errorf("service %s not found: %w", serviceArn, ErrNotFound)
	}

	all := make([]*CustomDomain, 0, len(b.customDomains[serviceArn]))
	for _, d := range b.customDomains[serviceArn] {
		cp := d.toCustomDomain()
		all = append(all, &cp)
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	dnsTarget := svc.ServiceURL

	return pg.Data, pg.Next, dnsTarget, nil
}
