package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveSNSAID resolves a SNSA identifier.
func (b *InMemoryBackend) resolveSNSAID(identifier string) (string, bool) {
	if b.snsas.Has(identifier) {
		return identifier, true
	}
	for _, s := range b.snsas.All() {
		if s.ARN == identifier {
			return s.ID, true
		}
	}

	return "", false
}

// resolveSNVAID resolves a SNVA identifier.
func (b *InMemoryBackend) resolveSNVAID(identifier string) (string, bool) {
	if b.snvas.Has(identifier) {
		return identifier, true
	}
	for _, s := range b.snvas.All() {
		if s.ARN == identifier {
			return s.ID, true
		}
	}

	return "", false
}

// ------- ServiceNetworkServiceAssociation operations -------

// CreateServiceNetworkServiceAssociation creates a service-to-network association.
func (b *InMemoryBackend) CreateServiceNetworkServiceAssociation(
	ctx context.Context,
	serviceNetworkID, serviceID string,
	tags map[string]string,
) (*ServiceNetworkServiceAssociation, error) {
	b.mu.Lock("CreateServiceNetworkServiceAssociation")
	defer b.mu.Unlock()

	snID, ok := b.resolveServiceNetworkID(serviceNetworkID)
	if !ok {
		return nil, ErrNotFound
	}

	svcID, ok := b.resolveServiceID(serviceID)
	if !ok {
		return nil, ErrNotFound
	}

	// check for existing association
	for _, s := range b.snsas.All() {
		if s.ServiceNetworkID == snID && s.ServiceID == svcID {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixSNSA)
	region := b.regionFor(ctx)
	assocARN := arn.Build(arnService, region, b.accountID, resourceServiceNetworkSvcAssoc+"/"+id)

	sn, _ := b.serviceNetworks.Get(snID)
	svc, _ := b.services.Get(svcID)

	snsa := &storedSNSA{
		ARN:                assocARN,
		ID:                 id,
		ServiceARN:         svc.ARN,
		ServiceID:          svcID,
		ServiceName:        svc.Name,
		ServiceNetworkARN:  sn.ARN,
		ServiceNetworkID:   snID,
		ServiceNetworkName: sn.Name,
		Status:             statusActive,
		CreatedBy:          b.accountID,
		CustomDomainName:   svc.CustomDomainName,
		DNSName:            svc.DNSName,
		HostedZoneID:       svc.HostedZoneID,
		Tags:               copyTags(tags),
		CreatedAt:          now,
		Region:             region,
	}

	b.snsas.Put(snsa)
	b.tags[assocARN] = copyTags(tags)

	return snsa.toAssociation(), nil
}

// GetServiceNetworkServiceAssociation returns a SNSA by ID or ARN.
func (b *InMemoryBackend) GetServiceNetworkServiceAssociation(
	snsaID string,
) (*ServiceNetworkServiceAssociation, error) {
	b.mu.RLock("GetServiceNetworkServiceAssociation")
	defer b.mu.RUnlock()

	id, ok := b.resolveSNSAID(snsaID)
	if !ok {
		return nil, ErrNotFound
	}

	s, _ := b.snsas.Get(id)

	return s.toAssociation(), nil
}

// DeleteServiceNetworkServiceAssociation deletes a SNSA.
func (b *InMemoryBackend) DeleteServiceNetworkServiceAssociation(snsaID string) error {
	b.mu.Lock("DeleteServiceNetworkServiceAssociation")
	defer b.mu.Unlock()

	id, ok := b.resolveSNSAID(snsaID)
	if !ok {
		return ErrNotFound
	}

	s, _ := b.snsas.Get(id)
	b.snsas.Delete(id)
	delete(b.tags, s.ARN)

	return nil
}

// ListServiceNetworkServiceAssociations lists SNSAs with optional filters.
func (b *InMemoryBackend) ListServiceNetworkServiceAssociations(
	ctx context.Context,
	serviceNetworkID, serviceID string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkServiceAssociationSummary, string, error) {
	b.mu.RLock("ListServiceNetworkServiceAssociations")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceNetworkServiceAssociationSummary, 0)

	for _, s := range b.snsas.All() {
		if s.Region != region {
			continue
		}

		if serviceNetworkID != "" && s.ServiceNetworkID != serviceNetworkID &&
			s.ServiceNetworkARN != serviceNetworkID {
			continue
		}

		if serviceID != "" && s.ServiceID != serviceID && s.ServiceARN != serviceID {
			continue
		}

		all = append(all, s.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- ServiceNetworkVpcAssociation operations -------

// CreateServiceNetworkVpcAssociation creates a VPC-to-network association.
func (b *InMemoryBackend) CreateServiceNetworkVpcAssociation(
	ctx context.Context,
	serviceNetworkID, vpcID string,
	securityGroupIDs []string,
	privateDNSEnabled bool,
	tags map[string]string,
) (*ServiceNetworkVpcAssociation, error) {
	if vpcID == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateServiceNetworkVpcAssociation")
	defer b.mu.Unlock()

	snID, ok := b.resolveServiceNetworkID(serviceNetworkID)
	if !ok {
		return nil, ErrNotFound
	}

	// check for existing
	for _, s := range b.snvas.All() {
		if s.ServiceNetworkID == snID && s.VpcID == vpcID {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixSNVA)
	region := b.regionFor(ctx)
	assocARN := arn.Build(arnService, region, b.accountID, resourceServiceNetworkVpcAssoc+"/"+id)

	sn, _ := b.serviceNetworks.Get(snID)
	sgs := make([]string, len(securityGroupIDs))
	copy(sgs, securityGroupIDs)

	snva := &storedSNVA{
		ARN:                assocARN,
		ID:                 id,
		VpcID:              vpcID,
		ServiceNetworkARN:  sn.ARN,
		ServiceNetworkID:   snID,
		ServiceNetworkName: sn.Name,
		SecurityGroupIDs:   sgs,
		Status:             statusActive,
		CreatedBy:          b.accountID,
		PrivateDNSEnabled:  privateDNSEnabled,
		Tags:               copyTags(tags),
		CreatedAt:          now,
		LastUpdatedAt:      now,
		Region:             region,
	}

	b.snvas.Put(snva)
	b.tags[assocARN] = copyTags(tags)

	return snva.toAssociation(), nil
}

// GetServiceNetworkVpcAssociation returns a SNVA.
func (b *InMemoryBackend) GetServiceNetworkVpcAssociation(
	snvaID string,
) (*ServiceNetworkVpcAssociation, error) {
	b.mu.RLock("GetServiceNetworkVpcAssociation")
	defer b.mu.RUnlock()

	id, ok := b.resolveSNVAID(snvaID)
	if !ok {
		return nil, ErrNotFound
	}

	s, _ := b.snvas.Get(id)

	return s.toAssociation(), nil
}

// UpdateServiceNetworkVpcAssociation updates security groups on a SNVA.
func (b *InMemoryBackend) UpdateServiceNetworkVpcAssociation(
	snvaID string,
	securityGroupIDs []string,
) (*ServiceNetworkVpcAssociation, error) {
	b.mu.Lock("UpdateServiceNetworkVpcAssociation")
	defer b.mu.Unlock()

	id, ok := b.resolveSNVAID(snvaID)
	if !ok {
		return nil, ErrNotFound
	}

	snva, _ := b.snvas.Get(id)
	sgs := make([]string, len(securityGroupIDs))
	copy(sgs, securityGroupIDs)
	snva.SecurityGroupIDs = sgs
	snva.LastUpdatedAt = time.Now().UTC()

	return snva.toAssociation(), nil
}

// DeleteServiceNetworkVpcAssociation deletes a SNVA.
func (b *InMemoryBackend) DeleteServiceNetworkVpcAssociation(snvaID string) error {
	b.mu.Lock("DeleteServiceNetworkVpcAssociation")
	defer b.mu.Unlock()

	id, ok := b.resolveSNVAID(snvaID)
	if !ok {
		return ErrNotFound
	}

	s, _ := b.snvas.Get(id)
	b.snvas.Delete(id)
	delete(b.tags, s.ARN)

	return nil
}

// ListServiceNetworkVpcAssociations lists SNVAs with optional filters.
func (b *InMemoryBackend) ListServiceNetworkVpcAssociations(
	ctx context.Context,
	serviceNetworkID, vpcID string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkVpcAssociationSummary, string, error) {
	b.mu.RLock("ListServiceNetworkVpcAssociations")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceNetworkVpcAssociationSummary, 0)

	for _, s := range b.snvas.All() {
		if s.Region != region {
			continue
		}

		if serviceNetworkID != "" && s.ServiceNetworkID != serviceNetworkID &&
			s.ServiceNetworkARN != serviceNetworkID {
			continue
		}

		if vpcID != "" && s.VpcID != vpcID {
			continue
		}

		all = append(all, s.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
