package appconfig

import (
	"maps"
)

// ListTagsForResource returns the tags for the given resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.tags[resourceArn]
	result := make(map[string]string, len(t))
	maps.Copy(result, t)

	return result, nil
}

// TagResource adds or replaces tags on the given resource ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes the specified tag keys from the given resource ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t := b.tags[resourceArn]
	if t == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingAppConfig).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every AppConfig resource ARN (applications,
// environments, configuration profiles, deployment strategies, extensions,
// extension associations, experiment definitions) that currently has at
// least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for arn, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
	}

	return out
}
