package memorydb

import (
	"context"
	"fmt"
	"maps"
	"sort"
)

// ListTags returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTags(_ context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region, ref, ok := b.findARN(resourceArn)
	if !ok {
		return nil, ErrInvalidARN
	}

	tags := b.tagsForRef(region, ref)

	return tags, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	region, ref, ok := b.findARN(resourceArn)
	if !ok {
		return ErrInvalidARN
	}

	const maxTagsPerResource = 50

	existingTags := b.tagsForRef(region, ref)
	newTotal := len(existingTags)

	for k := range tags {
		if _, alreadyExists := existingTags[k]; !alreadyExists {
			newTotal++
		}
	}

	if newTotal > maxTagsPerResource {
		return fmt.Errorf(
			"tag limit exceeded: a resource may have at most %d tags: %w",
			maxTagsPerResource,
			ErrValidation,
		)
	}

	b.applyTags(region, ref, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	region, ref, ok := b.findARN(resourceArn)
	if !ok {
		return ErrInvalidARN
	}

	b.removeTags(region, ref, tagKeys)

	return nil
}

// findARN searches all regions' arnToResource maps for the given ARN.
func (b *InMemoryBackend) findARN(resourceArn string) (string, resourceRef, bool) {
	for region, store := range b.arnToResource {
		if ref, ok := store[resourceArn]; ok {
			return region, ref, true
		}
	}

	return "", resourceRef{}, false
}

// tagsForRef returns a copy of the tags for the referenced resource (must hold at least RLock).
func (b *InMemoryBackend) tagsForRef(region string, ref resourceRef) map[string]string {
	var src map[string]string

	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := tableGet(b.clusters[region], ref.Name); ok {
			src = c.Tags
		}
	case resourceKindACL:
		if a, ok := tableGet(b.acls[region], ref.Name); ok {
			src = a.Tags
		}
	case resourceKindSubnetGroup:
		if sg, ok := tableGet(b.subnetGroups[region], ref.Name); ok {
			src = sg.Tags
		}
	case resourceKindUser:
		if u, ok := tableGet(b.users[region], ref.Name); ok {
			src = u.Tags
		}
	case resourceKindParameterGroup:
		if pg, ok := tableGet(b.parameterGroups[region], ref.Name); ok {
			src = pg.Tags
		}
	case resourceKindSnapshot:
		if s, ok := tableGet(b.snapshots[region], ref.Name); ok {
			src = s.Tags
		}
	case resourceKindMultiRegionCluster:
		if mrc, ok := b.multiRegionClusters.Get(ref.Name); ok {
			src = mrc.Tags
		}
	}

	return maps.Clone(src)
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingMemoryDB).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every MemoryDB resource ARN (clusters, ACLs,
// subnet groups, users, parameter groups, snapshots, multi-region clusters), across all regions,
// that currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.arnToResource))

	for region, arns := range b.arnToResource {
		for resourceArn, ref := range arns {
			t := b.tagsForRef(region, ref)
			if len(t) == 0 {
				continue
			}

			out = append(out, TaggedEntry{ARN: resourceArn, Tags: t})
		}
	}

	return out
}

// mergeTags ensures dst is initialized then copies all src entries into it.
func mergeTags(dst *map[string]string, src map[string]string) {
	if *dst == nil {
		*dst = make(map[string]string, len(src))
	}

	maps.Copy(*dst, src)
}

func (b *InMemoryBackend) applyTags(region string, ref resourceRef, tags map[string]string) {
	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := tableGet(b.clusters[region], ref.Name); ok {
			mergeTags(&c.Tags, tags)
		}
	case resourceKindACL:
		if a, ok := tableGet(b.acls[region], ref.Name); ok {
			mergeTags(&a.Tags, tags)
		}
	case resourceKindSubnetGroup:
		if sg, ok := tableGet(b.subnetGroups[region], ref.Name); ok {
			mergeTags(&sg.Tags, tags)
		}
	case resourceKindUser:
		if u, ok := tableGet(b.users[region], ref.Name); ok {
			mergeTags(&u.Tags, tags)
		}
	case resourceKindParameterGroup:
		if pg, ok := tableGet(b.parameterGroups[region], ref.Name); ok {
			mergeTags(&pg.Tags, tags)
		}
	case resourceKindSnapshot:
		if s, ok := tableGet(b.snapshots[region], ref.Name); ok {
			mergeTags(&s.Tags, tags)
		}
	case resourceKindMultiRegionCluster:
		if mrc, ok := b.multiRegionClusters.Get(ref.Name); ok {
			mergeTags(&mrc.Tags, tags)
		}
	}
}

// removeTags deletes the given tag keys from the referenced resource (must hold Lock).
func (b *InMemoryBackend) removeTags(region string, ref resourceRef, tagKeys []string) {
	m := b.tagsMapForRef(region, ref)
	if m == nil {
		return
	}

	for _, k := range tagKeys {
		delete(m, k)
	}
}

// tagsMapForRef returns a direct (mutable) reference to the tag map for a resource (must hold Lock).
func (b *InMemoryBackend) tagsMapForRef(region string, ref resourceRef) map[string]string {
	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := tableGet(b.clusters[region], ref.Name); ok {
			return c.Tags
		}
	case resourceKindACL:
		if a, ok := tableGet(b.acls[region], ref.Name); ok {
			return a.Tags
		}
	case resourceKindSubnetGroup:
		if sg, ok := tableGet(b.subnetGroups[region], ref.Name); ok {
			return sg.Tags
		}
	case resourceKindUser:
		if u, ok := tableGet(b.users[region], ref.Name); ok {
			return u.Tags
		}
	case resourceKindParameterGroup:
		if pg, ok := tableGet(b.parameterGroups[region], ref.Name); ok {
			return pg.Tags
		}
	case resourceKindSnapshot:
		if s, ok := tableGet(b.snapshots[region], ref.Name); ok {
			return s.Tags
		}
	case resourceKindMultiRegionCluster:
		if mrc, ok := b.multiRegionClusters.Get(ref.Name); ok {
			return mrc.Tags
		}
	}

	return nil
}

// -- Snapshot operations --------------------------------------------------------

// tagsFromSlice converts []tagEntry to map[string]string.
func tagsFromSlice(tags []tagEntry) map[string]string {
	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

// tagsToSlice converts map[string]string to []tagEntry sorted by key.
func tagsToSlice(tags map[string]string) []tagEntry {
	result := make([]tagEntry, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagEntry{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}
