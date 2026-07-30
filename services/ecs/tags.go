package ecs

import (
	"fmt"
	"strings"
)

// resourceTagKey returns the tags-map key for a resource ARN.
func resourceTagKey(resourceArn string) string { return resourceArn }

// wantsIncludeTag reports whether opts (a DescribeX request's `include` list)
// requests resource tags, matching AWS's case-insensitive "TAGS" sentinel.
// Shared by every Describe* handler that gates tags behind Include=[TAGS]
// (Clusters, ContainerInstances, Services, TaskDefinition, TaskSet,
// ExpressGatewayService).
func wantsIncludeTag(opts []string, want string) bool {
	for _, opt := range opts {
		if strings.EqualFold(opt, want) {
			return true
		}
	}

	return false
}

// attachTagsIfWanted builds views for a list of ARN-identified resources,
// attaching each one's current tags via ListTagsForResource only when
// wantTags is set (real AWS's Include=[TAGS] gating). Shared by
// DescribeServices and DescribeContainerInstances to avoid two
// near-identical "build view, optionally look up and attach tags" loops.
func attachTagsIfWanted[T, V any](
	h *Handler,
	items []T,
	arnOf func(T) string,
	toView func(T) V,
	setTags func(*V, []Tag),
	wantTags bool,
) ([]V, error) {
	views := make([]V, 0, len(items))

	for _, item := range items {
		v := toView(item)

		if wantTags {
			tags, err := h.Backend.ListTagsForResource(arnOf(item))
			if err != nil {
				return nil, err
			}

			setTags(&v, tags)
		}

		views = append(views, v)
	}

	return views, nil
}

// copyTags returns a deep copy of the given tag slice.
func copyTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out
}

// TagResource applies key/value tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags []Tag) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.setResourceTagsLocked(resourceArn, tags)

	return nil
}

// setResourceTagsLocked merges tags into the resourceTags side map for a
// resource ARN, overwriting any existing tag with the same key. Shared by
// TagResource and resource-creation ops (for example CreateCluster) that
// accept an initial set of tags while already holding the write lock. Must be
// called with the write lock held.
func (b *InMemoryBackend) setResourceTagsLocked(resourceArn string, tags []Tag) {
	if b.resourceTags == nil {
		b.resourceTags = make(map[string][]Tag)
	}

	existing := b.resourceTags[resourceTagKey(resourceArn)]
	// Merge: existing tags with same key are overwritten.
	tagMap := make(map[string]string, len(existing)+len(tags))

	for _, t := range existing {
		tagMap[t.Key] = t.Value
	}

	for _, t := range tags {
		tagMap[t.Key] = t.Value
	}

	merged := make([]Tag, 0, len(tagMap))

	for k, v := range tagMap {
		merged = append(merged, Tag{Key: k, Value: v})
	}

	b.resourceTags[resourceTagKey(resourceArn)] = merged
}

// deleteResourceTagsLocked removes the resourceTags side-map entry for a
// resource ARN, if any. Call this from every resource-delete path that a
// client could have tagged via TagResource (clusters, services, container
// instances, task sets, task definitions, daemons, daemon task definitions,
// express gateway services) so a delete+recreate cycle with the same
// deterministic ARN does not resurrect stale tags, and so random-ID ARNs
// (task sets, tasks) do not leak a permanent resourceTags row after their
// owning resource is gone. Must be called with the write lock held.
func (b *InMemoryBackend) deleteResourceTagsLocked(resourceArn string) {
	if b.resourceTags == nil {
		return
	}

	delete(b.resourceTags, resourceTagKey(resourceArn))
}

// UntagResource removes tags with the given keys from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		return nil
	}

	key := resourceTagKey(resourceArn)
	existing := b.resourceTags[key]

	if len(existing) == 0 {
		return nil
	}

	keySet := make(map[string]struct{}, len(tagKeys))

	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	filtered := existing[:0]

	for _, t := range existing {
		if _, remove := keySet[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}

	b.resourceTags[key] = filtered

	return nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingECS).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every ECS resource ARN that currently has at least one
// tag applied via TagResource, spanning every ECS resource kind that shares the
// flat resourceTags side map (clusters, services, task definitions, container
// instances, task sets, capacity providers, and more).
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))

	for arn, tagList := range b.resourceTags {
		if len(tagList) == 0 {
			continue
		}

		tagMap := make(map[string]string, len(tagList))
		for _, t := range tagList {
			tagMap[t.Key] = t.Value
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: tagMap})
	}

	return out
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) ([]Tag, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.resourceTags == nil {
		return []Tag{}, nil
	}

	tags := b.resourceTags[resourceTagKey(resourceArn)]

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out, nil
}
