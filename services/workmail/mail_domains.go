package workmail

import (
	"fmt"
	"sort"
	"strings"
)

// --- Mail Domains ---

// dkimRecordCount is the number of DKIM CNAME records WorkMail recommends
// per domain (matches real WorkMail's per-domain DKIM token count).
// fixedRecordCount is the number of non-DKIM records dnsRecordsForDomain
// always emits (MX, SPF TXT, autodiscover CNAME).
const (
	dkimRecordCount  = 3
	fixedRecordCount = 3
)

// dnsRecordsForDomain builds the recommended DNS record list a real
// GetMailDomain response includes (see types.DnsRecord /
// GetMailDomainOutput.Records): an MX record routing inbound mail to SES, an
// SPF TXT record, an autodiscover CNAME, and dkimRecordCount DKIM CNAMEs.
// Token values are simulation-only placeholders (real WorkMail issues
// per-domain random DKIM tokens); the wire shape ({Hostname,Type,Value} per
// entry) is what a real SDK client actually reads.
func dnsRecordsForDomain(domainName, region string) []DNSRecord {
	records := make([]DNSRecord, 0, fixedRecordCount+dkimRecordCount)
	records = append(records,
		DNSRecord{Hostname: domainName, Type: "MX", Value: fmt.Sprintf("10 inbound-smtp.%s.amazonaws.com", region)},
		DNSRecord{Hostname: domainName, Type: "TXT", Value: `"v=spf1 include:amazonses.com ~all"`},
		DNSRecord{
			Hostname: "autodiscover." + domainName,
			Type:     "CNAME",
			Value:    "autodiscover.mail." + region + ".awsapps.com",
		},
	)
	for range dkimRecordCount {
		token := strings.ReplaceAll(newID(), "-", "")[:32]
		records = append(records, DNSRecord{
			Hostname: token + "._domainkey." + domainName,
			Type:     "CNAME",
			Value:    token + ".dkim.amazonses.com",
		})
	}

	return records
}

// RegisterMailDomain registers a domain with the organization.
func (b *InMemoryBackend) RegisterMailDomain(orgID, domainName string) error {
	b.mu.Lock("RegisterMailDomain")
	defer b.mu.Unlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if b.mailDomains.Has(orgKey(orgID, domainName)) {
		return fmt.Errorf("%w: domain %q already registered", ErrMailDomainInUse, domainName)
	}

	region := org.Region
	if region == "" {
		region = b.region
	}

	b.mailDomains.Put(&MailDomain{
		DomainName:                  domainName,
		IsDefault:                   false,
		IsTestDomain:                false,
		OwnershipVerificationStatus: dnsVerificationPending,
		DkimVerificationStatus:      dnsVerificationPending,
		Records:                     dnsRecordsForDomain(domainName, region),
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
