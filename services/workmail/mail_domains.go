package workmail

import (
	"fmt"
	"sort"
)

// --- Mail Domains ---

// RegisterMailDomain registers a domain with the organization.
func (b *InMemoryBackend) RegisterMailDomain(orgID, domainName string) error {
	b.mu.Lock("RegisterMailDomain")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if b.mailDomains.Has(orgKey(orgID, domainName)) {
		return fmt.Errorf("%w: domain %q already registered", ErrConflict, domainName)
	}

	b.mailDomains.Put(&MailDomain{
		DomainName:                  domainName,
		IsDefault:                   false,
		IsTestDomain:                false,
		OwnershipVerificationStatus: "PENDING",
		orgID:                       orgID,
	})

	return nil
}

// DeregisterMailDomain removes a domain from the organization.
func (b *InMemoryBackend) DeregisterMailDomain(orgID, domainName string) error {
	b.mu.Lock("DeregisterMailDomain")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	domain, exists := b.mailDomains.Get(orgKey(orgID, domainName))
	if !exists {
		return fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}
	if domain.IsDefault {
		return fmt.Errorf("%w: cannot deregister the default domain", ErrMailDomainState)
	}
	b.mailDomains.Delete(orgKey(orgID, domainName))

	return nil
}

// GetMailDomain returns details about a registered domain.
func (b *InMemoryBackend) GetMailDomain(orgID, domainName string) (*MailDomain, error) {
	b.mu.RLock("GetMailDomain")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	d, exists := b.mailDomains.Get(orgKey(orgID, domainName))
	if !exists {
		return nil, fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}

	return d, nil
}

// ListMailDomains returns a paginated list of mail domains.
func (b *InMemoryBackend) ListMailDomains(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*MailDomainSummary, string, error) {
	b.mu.RLock("ListMailDomains")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	domainsByOrg := b.mailDomainsByOrg.Get(orgID)
	domains := make([]*MailDomainSummary, 0, len(domainsByOrg))
	for _, d := range domainsByOrg {
		domains = append(domains, &MailDomainSummary{
			DomainName:   d.DomainName,
			IsDefault:    d.IsDefault,
			IsTestDomain: d.IsTestDomain,
		})
	}
	sort.Slice(
		domains,
		func(i, j int) bool { return domains[i].DomainName < domains[j].DomainName },
	)

	items, next := paginate(domains, maxResults, nextToken)

	return items, next, nil
}

// UpdateDefaultMailDomain changes the default mail domain.
func (b *InMemoryBackend) UpdateDefaultMailDomain(orgID, domainName string) error {
	b.mu.Lock("UpdateDefaultMailDomain")
	defer b.mu.Unlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	d, exists := b.mailDomains.Get(orgKey(orgID, domainName))
	if !exists {
		return fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}
	// clear old default
	for _, dom := range b.mailDomainsByOrg.Get(orgID) {
		dom.IsDefault = false
	}
	d.IsDefault = true
	org.DefaultMailDomain = domainName

	return nil
}
