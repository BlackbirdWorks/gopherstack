package cognitoidp

import "maps"

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)
}

// UntagResource removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		return
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[arn], k)
	}
}

// ListTagsForResource returns a copy of the tag map for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return maps.Clone(b.resourceTags[arn])
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingCognitoIDP).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Cognito user pool ARN that currently has at
// least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))

	for arn, t := range b.resourceTags {
		if len(t) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
	}

	return out
}
