package amplify

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// clone deep-copies da's SubDomains slice. A shallow "cp := *da" is not
// enough: the janitor's advanceDomains mutates domain.SubDomains[i].Verified
// in place through the live pointer rather than replacing the slice, so any
// copy that still aliases the backing array stays exposed to that mutation
// after it crosses the lock boundary -- see TestDomainAssociationSubDomainsRace.
func (da *DomainAssociation) clone() *DomainAssociation {
	cp := *da
	cp.SubDomains = append([]SubDomain(nil), da.SubDomains...)

	return &cp
}

// CreateDomainAssociation creates a custom domain association for an app.
func (b *InMemoryBackend) CreateDomainAssociation(
	appID, domainName string,
	subDomains []SubDomainSetting,
	enableAutoSubDomain bool,
) (*DomainAssociation, error) {
	b.mu.Lock("CreateDomainAssociation")
	defer b.mu.Unlock()

	if !b.apps.Has(appID) {
		return nil, fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	key := domainKey(appID, domainName)
	if b.domains.Has(key) {
		return nil, fmt.Errorf(
			"%w: domain %s already exists for app %s",
			ErrAlreadyExists,
			domainName,
			appID,
		)
	}

	domARN := arn.Build(
		"amplify",
		b.region,
		b.accountID,
		fmt.Sprintf("apps/%s/domains/%s", appID, domainName),
	)

	subs := make([]SubDomain, 0, len(subDomains))

	for _, s := range subDomains {
		subs = append(subs, SubDomain{
			SubDomainSetting: s,
			Verified:         false,
			DNSRecord:        s.Prefix + "." + domainName + " CNAME " + appID + ".amplifyapp.com",
		})
	}

	da := &DomainAssociation{
		AppID:                            appID,
		DomainName:                       domainName,
		ARN:                              domARN,
		DomainStatus:                     DomainStatusPendingVerification,
		SubDomains:                       subs,
		EnableAutoSubDomain:              enableAutoSubDomain,
		CertificateVerificationDNSRecord: "_verify." + domainName + " CNAME _acm." + appID + ".amplifyapp.com",
	}

	b.domains.Put(da)

	return da.clone(), nil
}

// UpdateDomainAssociation updates a domain association.
func (b *InMemoryBackend) UpdateDomainAssociation(
	appID, domainName string,
	subDomains []SubDomainSetting,
	enableAutoSubDomain bool,
) (*DomainAssociation, error) {
	b.mu.Lock("UpdateDomainAssociation")
	defer b.mu.Unlock()

	da, err := b.findDomain(appID, domainName)
	if err != nil {
		return nil, err
	}

	subs := make([]SubDomain, 0, len(subDomains))

	for _, s := range subDomains {
		subs = append(subs, SubDomain{
			SubDomainSetting: s,
			Verified:         false,
			DNSRecord:        s.Prefix + "." + domainName + " CNAME " + appID + ".amplifyapp.com",
		})
	}

	da.SubDomains = subs
	da.EnableAutoSubDomain = enableAutoSubDomain

	return da.clone(), nil
}

// DeleteDomainAssociation deletes a domain association.
func (b *InMemoryBackend) DeleteDomainAssociation(
	appID, domainName string,
) (*DomainAssociation, error) {
	b.mu.Lock("DeleteDomainAssociation")
	defer b.mu.Unlock()

	da, err := b.findDomain(appID, domainName)
	if err != nil {
		return nil, err
	}

	cp := da.clone()
	b.domains.Delete(domainKey(appID, domainName))

	return cp, nil
}

// GetDomainAssociation returns a domain association.
func (b *InMemoryBackend) GetDomainAssociation(
	appID, domainName string,
) (*DomainAssociation, error) {
	b.mu.RLock("GetDomainAssociation")
	defer b.mu.RUnlock()

	da, err := b.findDomain(appID, domainName)
	if err != nil {
		return nil, err
	}

	return da.clone(), nil
}

// ListDomainAssociations lists domain associations for an app.
func (b *InMemoryBackend) ListDomainAssociations(
	appID, nextToken string,
	maxResults int,
) ([]*DomainAssociation, string, error) {
	b.mu.RLock("ListDomainAssociations")
	defer b.mu.RUnlock()

	if !b.apps.Has(appID) {
		return nil, "", fmt.Errorf("%w: app %s not found", ErrNotFound, appID)
	}

	var all []*DomainAssociation

	for _, da := range b.domainsByApp.Get(appID) {
		all = append(all, da.clone())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DomainName < all[j].DomainName })

	page, token := amplifyPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// findDomain locates a domain association. Must be called while holding a lock.
func (b *InMemoryBackend) findDomain(appID, domainName string) (*DomainAssociation, error) {
	da, ok := b.domains.Get(domainKey(appID, domainName))
	if !ok {
		return nil, fmt.Errorf("%w: domain %s not found for app %s", ErrNotFound, domainName, appID)
	}

	return da, nil
}
