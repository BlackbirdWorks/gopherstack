package dlm

import "maps"

// TagResource applies tags to a DLM resource ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	p, ok := b.findPolicyByARNLocked(resourceARN)
	if !ok {
		return ErrPolicyNotFound
	}

	maps.Copy(p.Tags, tags)

	return nil
}

// UntagResource removes tags from a DLM resource ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	p, ok := b.findPolicyByARNLocked(resourceARN)
	if !ok {
		return ErrPolicyNotFound
	}

	for _, k := range tagKeys {
		delete(p.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a DLM resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	p, ok := b.findPolicyByARNLocked(resourceARN)
	if !ok {
		return nil, ErrPolicyNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, p.Tags)

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every DLM policy ARN that currently has at least one tag
// applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	b.policies.Range(func(p *storedPolicy) bool {
		if len(p.Tags) > 0 {
			result := make(map[string]string, len(p.Tags))
			maps.Copy(result, p.Tags)
			out = append(out, TaggedEntry{ARN: p.PolicyArn, Tags: result})
		}

		return true
	})

	return out
}
