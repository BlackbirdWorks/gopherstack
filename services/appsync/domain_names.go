package appsync

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// isValidDomainName returns true if the given domain name looks like a valid DNS name.
func isValidDomainName(domain string) bool {
	n := len(domain)
	if n == 0 || n > 253 {
		return false
	}

	// Must not have leading or trailing dots.
	if domain[0] == '.' || domain[n-1] == '.' {
		return false
	}

	// Must contain at least one dot (ruling out bare hostnames).
	return strings.Contains(domain, ".")
}

// CreateDomainName creates a custom domain name.
func (b *InMemoryBackend) CreateDomainName(
	domainName, certificateARN, description string,
	tagMap map[string]string,
) (*DomainName, error) {
	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if !isValidDomainName(domainName) {
		return nil, fmt.Errorf("%w: invalid domain name %q", ErrValidation, domainName)
	}

	if b.domainNames.Has(domainName) {
		return nil, fmt.Errorf("%w: domain name %s already exists", ErrAlreadyExists, domainName)
	}

	domainNameARN := arn.Build("appsync", b.region, b.accountID, "domainnames/"+domainName)

	dn := &DomainName{
		DomainName:     domainName,
		CertificateARN: certificateARN,
		Description:    description,
		Tags:           tagMap,
		AppsyncDomain:  domainName + ".appsync-api." + b.region + ".amazonaws.com",
		HostedZoneID:   "Z2FDTNDATAQYW2",
		DomainNameARN:  domainNameARN,
	}

	b.domainNames.Put(dn)

	cp := *dn

	return &cp, nil
}

// AssociateAPI associates an API with a custom domain name.
func (b *InMemoryBackend) AssociateAPI(domainName, apiID string) (*APIAssociation, error) {
	b.mu.Lock("AssociateAPI")
	defer b.mu.Unlock()

	dn, ok := b.domainNames.Get(domainName)
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	// Validate that the API being associated exists.
	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	assoc := &APIAssociation{
		DomainName:        domainName,
		APIID:             apiID,
		AssociationStatus: "SUCCESS",
	}

	b.apiAssociations.Put(assoc)

	// Update the domain name record to reflect the associated API.
	dn.APIID = apiID

	cp := *assoc

	return &cp, nil
}

// GetDomainName returns a custom domain name configuration.
func (b *InMemoryBackend) GetDomainName(domainName string) (*DomainName, error) {
	b.mu.RLock("GetDomainName")
	defer b.mu.RUnlock()

	dn, ok := b.domainNames.Get(domainName)
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	cp := *dn

	return &cp, nil
}

// ListDomainNames returns all custom domain name configurations.
func (b *InMemoryBackend) ListDomainNames() ([]*DomainName, error) {
	b.mu.RLock("ListDomainNames")
	defer b.mu.RUnlock()

	dns := b.domainNames.All()
	out := make([]*DomainName, 0, len(dns))

	for _, dn := range dns {
		cp := *dn
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *DomainName) int {
		return strings.Compare(a.DomainName, b.DomainName)
	})

	return out, nil
}

// DeleteDomainName deletes a custom domain name configuration and its API association.
func (b *InMemoryBackend) DeleteDomainName(domainName string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	b.domainNames.Delete(domainName)
	b.apiAssociations.Delete(domainName)

	return nil
}

// GetAPIAssociation returns the API association for a domain name.
func (b *InMemoryBackend) GetAPIAssociation(domainName string) (*APIAssociation, error) {
	b.mu.RLock("GetApiAssociation")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	assoc, ok := b.apiAssociations.Get(domainName)
	if !ok {
		return &APIAssociation{
			DomainName:        domainName,
			AssociationStatus: "NOT_FOUND",
		}, nil
	}

	cp := *assoc

	return &cp, nil
}

// UpdateDomainName updates an existing custom domain name configuration.
func (b *InMemoryBackend) UpdateDomainName(domainName, description, certificateARN string) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	dn, ok := b.domainNames.Get(domainName)
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	if description != "" {
		dn.Description = description
	}

	if certificateARN != "" {
		dn.CertificateARN = certificateARN
	}

	cp := *dn

	return &cp, nil
}

// DisassociateAPI removes the API association from a domain name.
func (b *InMemoryBackend) DisassociateAPI(domainName string) error {
	b.mu.Lock("DisassociateApi")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	if !b.apiAssociations.Has(domainName) {
		return fmt.Errorf("%w: no api associated with domain %s", ErrNotFound, domainName)
	}

	// Clear APIID from domain name and remove association.
	if dn, ok := b.domainNames.Get(domainName); ok {
		dn.APIID = ""
	}

	b.apiAssociations.Delete(domainName)

	return nil
}
