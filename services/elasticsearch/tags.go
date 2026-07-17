package elasticsearch

import (
	"context"
	"fmt"
)

// ListTags returns tags for the domain identified by ARN. The region is resolved
// from the ARN, falling back to the ctx region.
func (b *InMemoryBackend) ListTags(ctx context.Context, domainARN string) (map[string]string, error) {
	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	d := b.findDomainByARN(region, domainARN)
	if d == nil {
		return nil, fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	return d.Tags.Clone(), nil
}

// AddTags adds or updates tags on the domain identified by ARN.
func (b *InMemoryBackend) AddTags(ctx context.Context, domainARN string, kv map[string]string) error {
	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(region, domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.Merge(kv)

	return nil
}

// RemoveTags removes tag keys from the domain identified by ARN.
func (b *InMemoryBackend) RemoveTags(ctx context.Context, domainARN string, keys []string) error {
	region := regionFromARN(domainARN, getRegion(ctx, b.region))
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	d := b.findDomainByARN(region, domainARN)
	if d == nil {
		return fmt.Errorf("%w: domain not found for ARN %s", ErrDomainNotFound, domainARN)
	}

	d.Tags.DeleteKeys(keys)

	return nil
}
