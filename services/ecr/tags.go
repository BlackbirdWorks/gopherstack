package ecr

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// TagResource associates tags with an ECR resource identified by its ARN.
func (b *InMemoryBackend) TagResource(
	ctx context.Context, //nolint:revive // existing issue.
	resourceArn string,
	tags map[string]string,
) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.findResourceTagsLocked(resourceArn)

	total := len(existing)

	for k := range tags {
		if _, ok := existing[k]; !ok {
			total++
		}
	}

	if total > maxTagsPerRepo {
		return fmt.Errorf("%w: The maximum number of tags (%d) per resource has been exceeded",
			ErrTooManyTags, maxTagsPerRepo)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from an ECR resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context, //nolint:revive // existing issue.
	resourceArn string,
	tagKeys []string,
) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.findResourceTagsLocked(resourceArn)
	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns all tags for an ECR resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context, //nolint:revive // existing issue.
	resourceArn string,
) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.repoTags[resourceArn]
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out, nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingECR).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every repository plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.repoTags))
	seen := make(map[string]struct{}, len(b.repoTags))

	for key, t := range b.repoTags {
		if len(t) == 0 {
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
	out := make([]string, 0, b.repos.Len())

	for _, v := range b.repos.All() {
		out = append(out, v.RepositoryARN)
	}

	return out
}

// findResourceTagsLocked returns (creating if absent) the tag map for the given ARN.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) findResourceTagsLocked(resourceArn string) map[string]string {
	if _, ok := b.repoTags[resourceArn]; !ok {
		b.repoTags[resourceArn] = make(map[string]string)
	}

	return b.repoTags[resourceArn]
}

// sortedTagKeys returns the keys of the given map sorted alphabetically.
func sortedTagKeys(tags map[string]string) []string {
	keys := collections.SortedKeys(tags)

	return keys
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}

	out := make(map[string]string, len(in))
	maps.Copy(out, in)

	return out
}
