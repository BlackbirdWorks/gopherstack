package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveALSID resolves an access log subscription identifier.
func (b *InMemoryBackend) resolveALSID(identifier string) (string, bool) {
	if b.alss.Has(identifier) {
		return identifier, true
	}
	for _, a := range b.alss.All() {
		if a.ARN == identifier {
			return a.ID, true
		}
	}

	return "", false
}

// ------- AccessLogSubscription operations -------

// CreateAccessLogSubscription creates an access log subscription.
func (b *InMemoryBackend) CreateAccessLogSubscription(
	ctx context.Context,
	resourceID, destinationArn, logType string,
	tags map[string]string,
) (*AccessLogSubscription, error) {
	if destinationArn == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateAccessLogSubscription")
	defer b.mu.Unlock()

	// resolve resource ID (service or service network)
	resourceARN := b.resolveResourceARN(resourceID)

	now := time.Now().UTC()
	id := newID(idPrefixALS)
	region := b.regionFor(ctx)
	alsARN := arn.Build(arnService, region, b.accountID, resourceAccessLogSubscription+"/"+id)

	als := &storedALS{
		ARN:                   alsARN,
		ID:                    id,
		ResourceARN:           resourceARN,
		ResourceID:            resourceID,
		DestinationARN:        destinationArn,
		ServiceNetworkLogType: logType,
		Tags:                  copyTags(tags),
		CreatedAt:             now,
		LastUpdatedAt:         now,
	}

	b.alss.Put(als)
	b.tags[alsARN] = copyTags(tags)

	return als.toALS(), nil
}

func (b *InMemoryBackend) resolveResourceARN(resourceID string) string {
	if svc, ok := b.services.Get(resourceID); ok {
		return svc.ARN
	}

	for _, svc := range b.services.All() {
		if svc.ARN == resourceID {
			return svc.ARN
		}
	}

	if sn, ok := b.serviceNetworks.Get(resourceID); ok {
		return sn.ARN
	}

	for _, sn := range b.serviceNetworks.All() {
		if sn.ARN == resourceID {
			return sn.ARN
		}
	}

	return resourceID
}

// GetAccessLogSubscription returns an access log subscription.
func (b *InMemoryBackend) GetAccessLogSubscription(alsID string) (*AccessLogSubscription, error) {
	b.mu.RLock("GetAccessLogSubscription")
	defer b.mu.RUnlock()

	id, ok := b.resolveALSID(alsID)
	if !ok {
		return nil, ErrNotFound
	}

	als, _ := b.alss.Get(id)

	return als.toALS(), nil
}

// UpdateAccessLogSubscription updates the destination ARN.
func (b *InMemoryBackend) UpdateAccessLogSubscription(
	alsID, destinationArn string,
) (*AccessLogSubscription, error) {
	b.mu.Lock("UpdateAccessLogSubscription")
	defer b.mu.Unlock()

	id, ok := b.resolveALSID(alsID)
	if !ok {
		return nil, ErrNotFound
	}

	als, _ := b.alss.Get(id)
	als.DestinationARN = destinationArn
	als.LastUpdatedAt = time.Now().UTC()

	return als.toALS(), nil
}

// DeleteAccessLogSubscription deletes an access log subscription.
func (b *InMemoryBackend) DeleteAccessLogSubscription(alsID string) error {
	b.mu.Lock("DeleteAccessLogSubscription")
	defer b.mu.Unlock()

	id, ok := b.resolveALSID(alsID)
	if !ok {
		return ErrNotFound
	}

	a, _ := b.alss.Get(id)
	b.alss.Delete(id)
	delete(b.tags, a.ARN)

	return nil
}

// ListAccessLogSubscriptions lists access log subscriptions for a resource.
func (b *InMemoryBackend) ListAccessLogSubscriptions(
	_ context.Context,
	resourceID string,
	maxResults int32,
	nextToken string,
) ([]*AccessLogSubscriptionSummary, string, error) {
	b.mu.RLock("ListAccessLogSubscriptions")
	defer b.mu.RUnlock()

	all := make([]*AccessLogSubscriptionSummary, 0)

	for _, a := range b.alss.All() {
		if resourceID != "" && a.ResourceID != resourceID && a.ResourceARN != resourceID {
			continue
		}

		all = append(all, a.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
