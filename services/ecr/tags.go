package ecr

import (
	"context"
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

// TaggedResources returns every ECR repository ARN that currently has at
// least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.repoTags))

	for arn, tagMap := range b.repoTags {
		if len(tagMap) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(tagMap)})
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
