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
// resource ARN, if any. Call this from every resource-delete path a client could
// have tagged via TagResource, or a delete+recreate cycle with the same
// deterministic ARN resurrects stale tags, and random-ID ARNs leak a permanent
// resourceTags row after their owning resource is gone. Must be called with the
// write lock held.
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

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every cluster, service, task definition, task, container instance, task set and
// capacity provider plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	seen := make(map[string]struct{}, len(b.resourceTags))

	for key, t := range b.resourceTags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: tagListToMap(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make(
		[]string,
		0,
		b.clusters.Len()+b.services.Len()+b.taskDefByArn.Len()+b.tasks.Len()+b.containerInstances.Len()+
			b.taskSets.Len()+b.capacityProviders.Len(),
	)

	for _, v := range b.clusters.All() {
		out = append(out, v.ClusterArn)
	}

	for _, v := range b.services.All() {
		out = append(out, v.ServiceArn)
	}

	for _, v := range b.taskDefByArn.All() {
		out = append(out, v.TaskDefinitionArn)
	}

	for _, v := range b.tasks.All() {
		out = append(out, v.TaskArn)
	}

	for _, v := range b.containerInstances.All() {
		out = append(out, v.ContainerInstanceArn)
	}

	for _, v := range b.taskSets.All() {
		out = append(out, v.TaskSetArn)
	}

	for _, v := range b.capacityProviders.All() {
		out = append(out, v.CapacityProviderArn)
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

func tagListToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}
