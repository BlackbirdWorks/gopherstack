package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveResourceGatewayID resolves a resource gateway identifier (ID or
// ARN) to an ID.
func (b *InMemoryBackend) resolveResourceGatewayID(identifier string) (string, bool) {
	if b.resourceGateways.Has(identifier) {
		return identifier, true
	}

	for _, g := range b.resourceGateways.All() {
		if g.ARN == identifier {
			return g.ID, true
		}
	}

	return "", false
}

// ------- ResourceGateway operations -------

// CreateResourceGateway creates a new resource gateway.
func (b *InMemoryBackend) CreateResourceGateway(
	ctx context.Context,
	name, vpcID, ipAddressType, resourceConfigDNSResolution string,
	ipv4AddressesPerENI int32,
	securityGroupIDs, subnetIDs []string,
	tags map[string]string,
) (*ResourceGateway, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateResourceGateway")
	defer b.mu.Unlock()

	if len(b.resourceGatewaysByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixResourceGateway)
	region := b.regionFor(ctx)
	gwARN := arn.Build(arnService, region, b.accountID, resourceResourceGateway+"/"+id)

	if ipAddressType == "" {
		ipAddressType = "IPV4"
	}

	if resourceConfigDNSResolution == "" {
		resourceConfigDNSResolution = "PUBLIC"
	}

	gw := &storedResourceGateway{
		ARN:                         gwARN,
		ID:                          id,
		Name:                        name,
		VpcID:                       vpcID,
		IPAddressType:               ipAddressType,
		ResourceConfigDNSResolution: resourceConfigDNSResolution,
		Ipv4AddressesPerEni:         ipv4AddressesPerENI,
		SecurityGroupIDs:            append([]string(nil), securityGroupIDs...),
		SubnetIDs:                   append([]string(nil), subnetIDs...),
		Status:                      statusActive,
		Tags:                        copyTags(tags),
		CreatedAt:                   now,
		LastUpdatedAt:               now,
		Region:                      region,
	}

	b.resourceGateways.Put(gw)
	b.tags[gwARN] = copyTags(tags)

	return gw.toResourceGateway(), nil
}

// GetResourceGateway returns a resource gateway.
func (b *InMemoryBackend) GetResourceGateway(id string) (*ResourceGateway, error) {
	b.mu.RLock("GetResourceGateway")
	defer b.mu.RUnlock()

	gwID, ok := b.resolveResourceGatewayID(id)
	if !ok {
		return nil, ErrNotFound
	}

	gw, _ := b.resourceGateways.Get(gwID)

	return gw.toResourceGateway(), nil
}

// UpdateResourceGateway updates a resource gateway's security groups -- the
// only field UpdateResourceGatewayInput accepts besides the identifier.
func (b *InMemoryBackend) UpdateResourceGateway(id string, securityGroupIDs []string) (*ResourceGateway, error) {
	b.mu.Lock("UpdateResourceGateway")
	defer b.mu.Unlock()

	gwID, ok := b.resolveResourceGatewayID(id)
	if !ok {
		return nil, ErrNotFound
	}

	gw, _ := b.resourceGateways.Get(gwID)
	gw.SecurityGroupIDs = append([]string(nil), securityGroupIDs...)
	gw.LastUpdatedAt = time.Now().UTC()

	return gw.toResourceGateway(), nil
}

// DeleteResourceGateway deletes a resource gateway. Real AWS rejects the
// delete with ConflictException while any resource configuration still
// references the gateway (CreateResourceConfiguration's
// ResourceGatewayIdentifier).
func (b *InMemoryBackend) DeleteResourceGateway(id string) (*ResourceGateway, error) {
	b.mu.Lock("DeleteResourceGateway")
	defer b.mu.Unlock()

	gwID, ok := b.resolveResourceGatewayID(id)
	if !ok {
		return nil, ErrNotFound
	}

	for _, c := range b.resourceConfigurations.All() {
		if c.ResourceGatewayID == gwID {
			return nil, ErrDependencyConflict
		}
	}

	gw, _ := b.resourceGateways.Get(gwID)
	out := gw.toResourceGateway()
	out.Status = statusDeleted

	b.resourceGateways.Delete(gwID)
	delete(b.tags, gw.ARN)

	return out, nil
}

// ListResourceGateways returns a paginated list of resource gateways.
func (b *InMemoryBackend) ListResourceGateways(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*ResourceGatewaySummary, string, error) {
	b.mu.RLock("ListResourceGateways")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ResourceGatewaySummary, 0, b.resourceGateways.Len())

	for _, gw := range b.resourceGateways.All() {
		if gw.Region != region {
			continue
		}

		all = append(all, gw.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
