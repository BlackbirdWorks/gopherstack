package apigateway

import (
	"fmt"
	"sort"
	"time"
)

// CreateDomainName creates a new custom domain name.
func (b *InMemoryBackend) CreateDomainName(input CreateDomainNameInput) (*DomainName, error) {
	if input.DomainName == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if b.domainNames.Has(input.DomainName) {
		return nil, fmt.Errorf("%w: domain name %q already exists", ErrAlreadyExists, input.DomainName)
	}

	now := unixEpochTime{time.Now()}
	backendTags := initTagsFromInput("apigw.domain."+input.DomainName+".tags", input.Tags)

	securityPolicy := input.SecurityPolicy
	if securityPolicy == "" {
		securityPolicy = "TLS_1_2"
	}

	endpointType := "REGIONAL"
	if input.EndpointConfiguration != nil && len(input.EndpointConfiguration.Types) > 0 {
		endpointType = input.EndpointConfiguration.Types[0]
	}

	var epConfig *EndpointConfiguration
	if input.EndpointConfiguration != nil {
		epConfig = input.EndpointConfiguration
	} else {
		epConfig = &EndpointConfiguration{Types: []string{endpointType}}
	}

	regionalDomain := input.DomainName + ".execute-api.us-east-1.amazonaws.com"
	distributionDomain := input.DomainName + ".cloudfront.net"

	dn := &DomainName{
		DomainNameValue:          input.DomainName,
		CertificateARN:           input.CertificateARN,
		RegionalCertificateARN:   input.RegionalCertificateARN,
		SecurityPolicy:           securityPolicy,
		EndpointConfiguration:    epConfig,
		RegionalDomainName:       regionalDomain,
		RegionalHostedZoneID:     "Z2FDTNDATAQYW2",
		DistributionDomainName:   distributionDomain,
		DistributionHostedZoneID: "Z2FDTNDATAQYW2",
		DomainNameStatus:         statusAvailable,
		Tags:                     backendTags,
		CreatedDate:              &now,
	}
	b.domainNames.Put(dn)

	cp := *dn

	return &cp, nil
}

// CreateDomainNameAccessAssociation creates an access association for a domain name.
func (b *InMemoryBackend) CreateDomainNameAccessAssociation(
	input CreateDomainNameAccessAssociationInput,
) (*DomainNameAccessAssociation, error) {
	if input.DomainNameARN == "" {
		return nil, fmt.Errorf("%w: domainNameArn is required", ErrInvalidParameter)
	}

	if input.AccessAssociationSource == "" {
		return nil, fmt.Errorf("%w: accessAssociationSource is required", ErrInvalidParameter)
	}

	if input.AccessAssociationSourceType == "" {
		return nil, fmt.Errorf("%w: accessAssociationSourceType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomainNameAccessAssociation")
	defer b.mu.Unlock()

	assocARN := "arn:aws:apigateway:us-east-1::/accessassociations/" + randomID(apiIDLength)
	assoc := &DomainNameAccessAssociation{
		DomainNameAccessAssociationARN: assocARN,
		DomainNameARN:                  input.DomainNameARN,
		AccessAssociationSource:        input.AccessAssociationSource,
		AccessAssociationSourceType:    input.AccessAssociationSourceType,
	}
	b.domainNameAccessAssociations.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// resourceOwnerSelf and resourceOwnerOther are the two valid values of the
// resourceOwner query parameter accepted by GetDomainNameAccessAssociations.
const (
	resourceOwnerSelf  = "SELF"
	resourceOwnerOther = "OTHER_ACCOUNTS"
)

// GetDomainNameAccessAssociations lists domain name access associations owned by
// this account. resourceOwner selects SELF (default) or OTHER_ACCOUNTS; since this
// backend only ever creates associations under the caller's own account,
// OTHER_ACCOUNTS always returns an empty list.
func (b *InMemoryBackend) GetDomainNameAccessAssociations(resourceOwner string) ([]DomainNameAccessAssociation, error) {
	b.mu.RLock("GetDomainNameAccessAssociations")
	defer b.mu.RUnlock()

	if resourceOwner == resourceOwnerOther {
		return []DomainNameAccessAssociation{}, nil
	}

	all := b.domainNameAccessAssociations.All()
	result := make([]DomainNameAccessAssociation, 0, len(all))
	for _, assoc := range all {
		result = append(result, *assoc)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DomainNameAccessAssociationARN < result[j].DomainNameAccessAssociationARN
	})

	return result, nil
}

// DeleteDomainNameAccessAssociation removes a domain name access association by ARN.
func (b *InMemoryBackend) DeleteDomainNameAccessAssociation(arn string) error {
	b.mu.Lock("DeleteDomainNameAccessAssociation")
	defer b.mu.Unlock()

	if !b.domainNameAccessAssociations.Delete(arn) {
		return fmt.Errorf("%w: domain name access association %s not found", ErrNotFound, arn)
	}

	return nil
}

// RejectDomainNameAccessAssociation rejects (removes) a domain name access
// association, validating that it belongs to the given domain name ARN.
func (b *InMemoryBackend) RejectDomainNameAccessAssociation(arn, domainNameARN string) error {
	b.mu.Lock("RejectDomainNameAccessAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.domainNameAccessAssociations.Get(arn)
	if !ok {
		return fmt.Errorf("%w: domain name access association %s not found", ErrNotFound, arn)
	}

	if domainNameARN != "" && assoc.DomainNameARN != domainNameARN {
		return fmt.Errorf("%w: domainNameArn does not match association %s", ErrInvalidParameter, arn)
	}

	b.domainNameAccessAssociations.Delete(arn)

	return nil
}

// GetDomainName retrieves a domain name by value.
func (b *InMemoryBackend) GetDomainName(name string) (*DomainName, error) {
	b.mu.RLock("GetDomainName")
	defer b.mu.RUnlock()
	dn, ok := b.domainNames.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrDomainNameNotFound, name)
	}
	cp := *dn

	return &cp, nil
}

// GetDomainNames returns all domain names sorted by name.
func (b *InMemoryBackend) GetDomainNames() ([]DomainName, error) {
	b.mu.RLock("GetDomainNames")
	defer b.mu.RUnlock()
	all := make([]DomainName, 0, b.domainNames.Len())
	for _, dn := range b.domainNames.All() {
		all = append(all, *dn)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DomainNameValue < all[j].DomainNameValue })

	return all, nil
}

// DeleteDomainName removes a domain name by value.
func (b *InMemoryBackend) DeleteDomainName(name string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()
	if !b.domainNames.Delete(name) {
		return fmt.Errorf("%w: domain name %s not found", ErrDomainNameNotFound, name)
	}

	return nil
}

// GetDomainNamesPage returns domain names with cursor-based pagination.
func (b *InMemoryBackend) GetDomainNamesPage(limit int, position string) ([]DomainName, string, error) {
	b.mu.RLock("GetDomainNamesPage")
	defer b.mu.RUnlock()

	all := make([]DomainName, 0, b.domainNames.Len())
	for _, d := range b.domainNames.All() {
		all = append(all, *d)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DomainNameValue < all[j].DomainNameValue })
	page, pos := paginatePageByKey(all, limit, position, func(d DomainName) string { return d.DomainNameValue })

	return page, pos, nil
}

// UpdateDomainName updates a domain name's certificate ARN.
func (b *InMemoryBackend) UpdateDomainName(input UpdateDomainNameInput) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	d, ok := b.domainNames.Get(input.DomainName)
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrDomainNameNotFound, input.DomainName)
	}

	if input.CertificateARN != "" {
		d.CertificateARN = input.CertificateARN
	}

	if input.RegionalCertificateARN != "" {
		d.RegionalCertificateARN = input.RegionalCertificateARN
	}

	if input.SecurityPolicy != "" {
		d.SecurityPolicy = input.SecurityPolicy
	}

	if input.EndpointConfiguration != nil {
		d.EndpointConfiguration = input.EndpointConfiguration
	}

	return d, nil
}
