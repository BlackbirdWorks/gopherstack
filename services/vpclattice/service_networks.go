package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveServiceNetworkID resolves a service network identifier to an ID.
func (b *InMemoryBackend) resolveServiceNetworkID(identifier string) (string, bool) {
	if b.serviceNetworks.Has(identifier) {
		return identifier, true
	}
	for _, sn := range b.serviceNetworks.All() {
		if sn.ARN == identifier || sn.Name == identifier {
			return sn.ID, true
		}
	}

	return "", false
}

// ------- ServiceNetwork operations -------

// CreateServiceNetwork creates a new service network.
func (b *InMemoryBackend) CreateServiceNetwork(
	ctx context.Context,
	name, authType string,
	tags map[string]string,
) (*ServiceNetwork, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateServiceNetwork")
	defer b.mu.Unlock()

	if len(b.networksByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixNetwork)
	region := b.regionFor(ctx)
	snARN := arn.Build(arnService, region, b.accountID, resourceServiceNetwork+"/"+id)

	if authType == "" {
		authType = authTypeNone
	}

	sn := &storedServiceNetwork{
		ARN:           snARN,
		ID:            id,
		Name:          name,
		AuthType:      authType,
		Tags:          copyTags(tags),
		CreatedAt:     now,
		LastUpdatedAt: now,
		Region:        region,
	}

	b.serviceNetworks.Put(sn)
	b.tags[snARN] = copyTags(tags)

	return sn.toServiceNetwork(), nil
}

// GetServiceNetwork returns a service network.
func (b *InMemoryBackend) GetServiceNetwork(snID string) (*ServiceNetwork, error) {
	b.mu.RLock("GetServiceNetwork")
	defer b.mu.RUnlock()

	id, ok := b.resolveServiceNetworkID(snID)
	if !ok {
		return nil, ErrNotFound
	}

	sn, _ := b.serviceNetworks.Get(id)

	// compute counts
	sn.NumberOfAssociatedServices = b.countSNSAs(id)
	sn.NumberOfAssociatedVPCs = b.countSNVAs(id)

	return sn.toServiceNetwork(), nil
}

func (b *InMemoryBackend) countSNSAs(snID string) int64 {
	var count int64
	for _, s := range b.snsas.All() {
		if s.ServiceNetworkID == snID {
			count++
		}
	}

	return count
}

func (b *InMemoryBackend) countSNVAs(snID string) int64 {
	var count int64
	for _, s := range b.snvas.All() {
		if s.ServiceNetworkID == snID {
			count++
		}
	}

	return count
}

func (b *InMemoryBackend) countSNRAs(snID string) int64 {
	var count int64
	for _, s := range b.snras.All() {
		if s.ServiceNetworkID == snID {
			count++
		}
	}

	return count
}

// UpdateServiceNetwork updates a service network.
func (b *InMemoryBackend) UpdateServiceNetwork(snID, authType string) (*ServiceNetwork, error) {
	b.mu.Lock("UpdateServiceNetwork")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceNetworkID(snID)
	if !ok {
		return nil, ErrNotFound
	}

	sn, _ := b.serviceNetworks.Get(id)
	if authType != "" {
		sn.AuthType = authType
	}

	sn.LastUpdatedAt = time.Now().UTC()

	return sn.toServiceNetwork(), nil
}

// DeleteServiceNetwork deletes a service network. Real AWS rejects the
// delete with ConflictException while any service or VPC is still
// associated with the service network, and otherwise cascades the delete
// through the service network's resource policy, auth policy, and access
// log subscriptions -- see the DeleteServiceNetwork doc comment in
// aws-sdk-go-v2/service/vpclattice's api_op_DeleteServiceNetwork.go.
func (b *InMemoryBackend) DeleteServiceNetwork(snID string) error {
	b.mu.Lock("DeleteServiceNetwork")
	defer b.mu.Unlock()

	id, ok := b.resolveServiceNetworkID(snID)
	if !ok {
		return ErrNotFound
	}

	if b.countSNSAs(id) > 0 || b.countSNVAs(id) > 0 || b.countSNRAs(id) > 0 {
		return ErrDependencyConflict
	}

	sn, _ := b.serviceNetworks.Get(id)
	b.serviceNetworks.Delete(id)
	delete(b.tags, sn.ARN)

	delete(b.authPolicies, sn.ARN)
	delete(b.resourcePolicies, sn.ARN)

	for _, a := range b.alss.All() {
		if a.ResourceARN == sn.ARN {
			b.alss.Delete(a.ID)
			delete(b.tags, a.ARN)
		}
	}

	return nil
}

// ListServiceNetworks returns a paginated list of service networks.
func (b *InMemoryBackend) ListServiceNetworks(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*ServiceNetworkSummary, string, error) {
	b.mu.RLock("ListServiceNetworks")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*ServiceNetworkSummary, 0, b.serviceNetworks.Len())

	for _, sn := range b.serviceNetworks.All() {
		if sn.Region != region {
			continue
		}

		all = append(all, sn.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
