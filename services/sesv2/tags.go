package sesv2

import "maps"

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.putResourceTagsLocked(arn, tags)

	return nil
}

// putResourceTagsLocked merges tags into the shared resourceTags store so
// ListTagsForResource sees them. Every Create* op that accepts inline tags
// must call this at creation time -- storing tags only on the resource's own
// domain struct (as CreateTenant/CreateEmailIdentity used to) left
// ListTagsForResource and cross-service GetResources blind to creation-time
// tags. Caller must hold b.mu.
func (b *InMemoryBackend) putResourceTagsLocked(arn string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)
}

func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	m := b.resourceTags[arn]
	for _, k := range tagKeys {
		delete(m, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	out := make(map[string]string, len(b.resourceTags[arn]))
	maps.Copy(out, b.resourceTags[arn])

	return out, nil
}

// liveTagsLocked returns a copy of the live tags for arn from the shared
// resourceTags store, rather than a resource's own creation-time Tags
// snapshot -- callers that echo tags back through a Get op must read this
// live map so tags applied via TagResource/UntagResource after creation are
// visible, matching the fix already applied for Create*'s inline tags (see
// putResourceTagsLocked). Caller must hold at least a read lock.
func (b *InMemoryBackend) liveTagsLocked(arn string) map[string]string {
	if len(b.resourceTags[arn]) == 0 {
		return nil
	}

	out := make(map[string]string, len(b.resourceTags[arn]))
	maps.Copy(out, b.resourceTags[arn])

	return out
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every SESv2 resource ARN that currently has at
// least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))

	for resourceArn, tags := range b.resourceTags {
		if len(tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceArn, Tags: maps.Clone(tags)})
	}

	return out
}
