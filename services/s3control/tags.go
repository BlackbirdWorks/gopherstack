package s3control

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

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

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every access point, Object Lambda access point, multi-region access point, access
// grant, location, instance and Storage Lens group plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	seen := make(map[string]struct{}, len(b.resourceTags))

	for key, t := range b.resourceTags {
		if key == "" || len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
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
		b.accessPoints.Len()+b.objectLambdaAccessPoints.Len()+b.mraps.Len()+b.accessGrants.Len()+
			b.accessGrantsLocations.Len()+b.accessGrantsInstances.Len()+b.storageLensGroups.Len(),
	)

	for _, v := range b.accessPoints.All() {
		out = append(out, v.AccessPointArn)
	}

	for _, v := range b.objectLambdaAccessPoints.All() {
		out = append(out, v.ObjectLambdaAccessPointArn)
	}

	for _, v := range b.mraps.All() {
		out = append(out, arn.Build("s3", "", v.AccountID, "accesspoint/"+v.Alias))
	}

	for _, v := range b.accessGrants.All() {
		out = append(out, v.AccessGrantArn)
	}

	for _, v := range b.accessGrantsLocations.All() {
		out = append(out, v.AccessGrantsLocationArn)
	}

	for _, v := range b.accessGrantsInstances.All() {
		out = append(out, v.AccessGrantsInstanceArn)
	}

	for _, v := range b.storageLensGroups.All() {
		out = append(out, v.StorageLensGroupArn)
	}

	return out
}
