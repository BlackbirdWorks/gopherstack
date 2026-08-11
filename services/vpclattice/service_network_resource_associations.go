package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveSNRAID resolves a service network resource association identifier
// (ID or ARN) to an ID.
func (b *InMemoryBackend) resolveSNRAID(identifier string) (string, bool) {
	if b.snras.Has(identifier) {
		return identifier, true
	}

	for _, s := range b.snras.All() {
		if s.ARN == identifier {
			return s.ID, true
		}
	}

	return "", false
}

// ------- ServiceNetworkResourceAssociation operations -------

// CreateServiceNetworkResourceAssociation associates a resource
// configuration with a service network.
func (b *InMemoryBackend) CreateServiceNetworkResourceAssociation(
	ctx context.Context,
	serviceNetworkIdentifier, resourceConfigurationIdentifier string,
	privateDNSEnabled bool,
	tags map[string]string,
) (*ServiceNetworkResourceAssociation, error) {
	b.mu.Lock("CreateServiceNetworkResourceAssociation")
	defer b.mu.Unlock()

	snID, ok := b.resolveServiceNetworkID(serviceNetworkIdentifier)
	if !ok {
		return nil, ErrNotFound
	}

	rcID, ok := b.resolveResourceConfigurationID(resourceConfigurationIdentifier)
	if !ok {
		return nil, ErrNotFound
	}

	for _, s := range b.snras.All() {
		if s.ServiceNetworkID == snID && s.ResourceConfigurationID == rcID {
			return nil, ErrAlreadyExists
		}
	}

	now := time.Now().UTC()
	id := newID(idPrefixServiceNetworkResAssoc)
	region := b.regionFor(ctx)
	assocARN := arn.Build(arnService, region, b.accountID, resourceServiceNetworkResAssoc+"/"+id)

	sn, _ := b.serviceNetworks.Get(snID)
	rc, _ := b.resourceConfigurations.Get(rcID)

	snra := &storedSNRA{
		ARN:                       assocARN,
		ID:                        id,
		ResourceConfigurationARN:  rc.ARN,
		ResourceConfigurationID:   rcID,
		ResourceConfigurationName: rc.Name,
		ServiceNetworkARN:         sn.ARN,
		ServiceNetworkID:          snID,
		ServiceNetworkName:        sn.Name,
		Status:                    statusActive,
		CreatedBy:                 b.accountID,
		PrivateDNSEnabled:         privateDNSEnabled,
		Tags:                      copyTags(tags),
		CreatedAt:                 now,
		LastUpdatedAt:             now,
		Region:                    region,
	}

	b.snras.Put(snra)
	b.tags[assocARN] = copyTags(tags)

	return snra.toAssociation(), nil
}

// GetServiceNetworkResourceAssociation returns a SNRA.
func (b *InMemoryBackend) GetServiceNetworkResourceAssociation(id string) (*ServiceNetworkResourceAssociation, error) {
	b.mu.RLock("GetServiceNetworkResourceAssociation")
	defer b.mu.RUnlock()

	snraID, ok := b.resolveSNRAID(id)
	if !ok {
		return nil, ErrNotFound
	}

	s, _ := b.snras.Get(snraID)

	return s.toAssociation(), nil
}

// DeleteServiceNetworkResourceAssociation deletes a SNRA.
func (b *InMemoryBackend) DeleteServiceNetworkResourceAssociation(
	id string,
) (*ServiceNetworkResourceAssociation, error) {
	b.mu.Lock("DeleteServiceNetworkResourceAssociation")
	defer b.mu.Unlock()

	snraID, ok := b.resolveSNRAID(id)
	if !ok {
		return nil, ErrNotFound
	}

	s, _ := b.snras.Get(snraID)
	out := s.toAssociation()
	out.Status = statusDeleteInProgress

	b.snras.Delete(snraID)
	delete(b.tags, s.ARN)

	return out, nil
}

// ListServiceNetworkResourceAssociations lists SNRAs with optional filters.
//
//nolint:dupl // structurally mirrors ListServiceNetworkServiceAssociations but filters a distinct table/type
func (b *InMemoryBackend) ListServiceNetworkResourceAssociations(
	ctx context.Context,
	serviceNetworkIdentifier, resourceConfigurationIdentifier string,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkResourceAssociationSummary, string, error) {
	b.mu.RLock("ListServiceNetworkResourceAssociations")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceNetworkResourceAssociationSummary, 0)

	for _, s := range b.snras.All() {
		if s.Region != region {
			continue
		}

		if serviceNetworkIdentifier != "" && s.ServiceNetworkID != serviceNetworkIdentifier &&
			s.ServiceNetworkARN != serviceNetworkIdentifier {
			continue
		}

		if resourceConfigurationIdentifier != "" && s.ResourceConfigurationID != resourceConfigurationIdentifier &&
			s.ResourceConfigurationARN != resourceConfigurationIdentifier {
			continue
		}

		all = append(all, s.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// ------- ResourceEndpointAssociation / ServiceNetworkVpcEndpointAssociation -------
//
// Both families are populated in real AWS exclusively by EC2
// CreateVpcEndpoint calls (VPC endpoints of type Resource or ServiceNetwork
// referencing a ResourceConfiguration/ServiceNetwork ARN) -- vpc-lattice
// itself exposes no Create operation for either. This backend has no EC2
// VPC-endpoint cross-service integration, so these lists are always empty:
// an honest reflection of "never created" (this backend genuinely never
// creates one), not a fabricated entry. See gopherstack-lx2k / PARITY.md.

// ListResourceEndpointAssociations always returns an empty page -- see the
// family doc comment above.
func (b *InMemoryBackend) ListResourceEndpointAssociations(
	_ context.Context,
	_ int32,
	_ string,
) ([]*ResourceEndpointAssociationSummary, string, error) {
	return []*ResourceEndpointAssociationSummary{}, "", nil
}

// DeleteResourceEndpointAssociation always reports not found -- no
// association can ever exist in this backend, see the family doc comment
// above.
func (b *InMemoryBackend) DeleteResourceEndpointAssociation(_ string) error {
	return ErrNotFound
}

// ListServiceNetworkVpcEndpointAssociations always returns an empty page --
// see the family doc comment above.
func (b *InMemoryBackend) ListServiceNetworkVpcEndpointAssociations(
	_ context.Context,
	_ string,
	_ int32,
	_ string,
) ([]*ServiceNetworkVpcEndpointAssociationSummary, string, error) {
	return []*ServiceNetworkVpcEndpointAssociationSummary{}, "", nil
}
