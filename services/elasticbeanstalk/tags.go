package elasticbeanstalk

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// sortedTagKeys returns the keys of a tags map in sorted order.
func sortedTagKeys(tags map[string]string) []string {
	keys := collections.SortedKeys(tags)

	return keys
}

// ListTagsForResource returns the tags for a resource identified by ARN.
// Tags are returned sorted by key for deterministic output.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if tags, ok := b.lookupTagsByARN(region, resourceARN); ok {
		return copyTags(tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN)
}

// UpdateTagsForResource updates tags on a resource identified by ARN.
func (b *InMemoryBackend) UpdateTagsForResource(
	ctx context.Context,
	resourceARN string,
	addTags, removeTags map[string]string,
) error {
	b.mu.Lock("UpdateTagsForResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	existing, ok := b.lookupTagsByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN)
	}

	if existing == nil {
		b.ensureTagsByARN(region, resourceARN)
		existing, _ = b.lookupTagsByARN(region, resourceARN)
	}

	maps.Copy(existing, addTags)

	for k := range removeTags {
		delete(existing, k)
	}

	return nil
}

// lookupTagsByARN looks up the tags map for a resource by ARN. Applications,
// environments, and application versions use O(1) index lookups;
// configuration templates and platform versions are supported too (AWS:
// "Elastic Beanstalk supports tagging of all of its resources") since both
// CreateConfigurationTemplate and CreatePlatformVersion accept a Tags
// parameter that must remain reachable through ListTagsForResource /
// UpdateTagsForResource.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupTagsByARN(region, resourceARN string) (map[string]string, bool) {
	if app, ok := b.applicationByARN(region, resourceARN); ok {
		return app.Tags, true
	}

	if env, ok := b.environmentByARN(region, resourceARN); ok {
		return env.Tags, true
	}

	if ver, ok := b.appVersionByARN(region, resourceARN); ok {
		return ver.Tags, true
	}

	if tmpl, ok := b.configTemplateByARN(region, resourceARN); ok {
		return tmpl.Tags, true
	}

	// PlatformVersion is keyed directly by its own ARN (see platformVersionKeyFn),
	// so no separate reverse index is needed.
	if pv, ok := b.platformVersionGet(region, resourceARN); ok {
		return pv.Tags, true
	}

	return nil, false
}

// ensureTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) ensureTagsByARN(region, resourceARN string) {
	if app, ok := b.applicationByARN(region, resourceARN); ok {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}

		return
	}

	if env, ok := b.environmentByARN(region, resourceARN); ok {
		if env.Tags == nil {
			env.Tags = make(map[string]string)
		}

		return
	}

	if ver, ok := b.appVersionByARN(region, resourceARN); ok {
		if ver.Tags == nil {
			ver.Tags = make(map[string]string)
		}

		return
	}

	if tmpl, ok := b.configTemplateByARN(region, resourceARN); ok {
		if tmpl.Tags == nil {
			tmpl.Tags = make(map[string]string)
		}

		return
	}

	if pv, ok := b.platformVersionGet(region, resourceARN); ok {
		if pv.Tags == nil {
			pv.Tags = make(map[string]string)
		}
	}
}
