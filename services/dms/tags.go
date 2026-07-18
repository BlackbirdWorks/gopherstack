package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// AddTagsToResource adds tags to a DMS resource by ARN.
func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, resourceArn string, kv map[string]string) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.Merge(kv)

	return nil
}

// ListTagsForResource returns tags for a DMS resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	return t.Clone(), nil
}

// findResourceTags returns the Tags for a resource ARN within the given region
// (must hold a lock). Returns nil if not found.
func (b *InMemoryBackend) findResourceTags(region, resourceArn string) *tags.Tags {
	if ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, resourceArn)); ok {
		return ri.Tags
	}

	if ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, resourceArn)); ok {
		return ep.Tags
	}

	if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, resourceArn)); ok {
		return rt.Tags
	}

	if dm, ok := lookupUnique(b.dataMigrationsByARN, regionKey(region, resourceArn)); ok {
		return dm.Tags
	}

	if dp, ok := lookupUnique(b.dataProvidersByARN, regionKey(region, resourceArn)); ok {
		return dp.Tags
	}

	if ip, ok := lookupUnique(b.instanceProfilesByARN, regionKey(region, resourceArn)); ok {
		return ip.Tags
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, resourceArn)); ok {
		return mp.Tags
	}

	if sg, ok := lookupUnique(b.replicationSubnetGroupsByARN, regionKey(region, resourceArn)); ok {
		return sg.Tags
	}

	if rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(region, resourceArn)); ok {
		return rc.Tags
	}

	return nil
}

// RemoveTagsFromResource removes tags from a DMS resource by ARN.
func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}
