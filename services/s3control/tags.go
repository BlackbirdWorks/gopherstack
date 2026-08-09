package s3control

import "maps"

// ---- Resource Tags ----

// ListTagsForResource returns all tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.resourceTags[arn]
	if tags == nil {
		return map[string]string{}
	}

	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// TagResource adds or updates tags on the given ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.resourceTags[arn]
	if existing == nil {
		existing = make(map[string]string, len(tags))
	}

	maps.Copy(existing, tags)
	b.resourceTags[arn] = existing
}

// UntagResource removes specific tag keys from the given ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	tags := b.resourceTags[arn]
	if tags == nil {
		return
	}

	for _, k := range tagKeys {
		delete(tags, k)
	}
}

// TaggedEntry pairs a resource ARN with its tag set, for the Resource Groups
// Tagging API's GetResources (see cli.go's wireTaggingS3Control).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every s3control resource with at least one tag,
// keyed by ARN. Only covers the resource kinds taggable through the generic
// TagResource/UntagResource/ListTagsForResource ops (access points, Object
// Lambda access points, multi-region access points, access grants) --
// batch job tags and Storage Lens configuration tags live in separate stores
// behind their own real, dedicated AWS ops (Put/Get/DeleteJobTagging,
// Put/GetStorageLensConfigurationTagging), not this generic surface.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	for resourceARN, tags := range b.resourceTags {
		if resourceARN == "" || len(tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceARN, Tags: maps.Clone(tags)})
	}

	return out
}
