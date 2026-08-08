package workmail

// --- Tags ---

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	merged := make([]Tag, 0, len(existing)+len(tags))
	merged = append(merged, existing...)
	for _, newTag := range tags {
		found := false
		for i, t := range merged {
			if t.Key == newTag.Key {
				merged[i].Value = newTag.Value
				found = true

				break
			}
		}
		if !found {
			merged = append(merged, newTag)
		}
	}
	b.tags[resourceARN] = merged

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	keySet := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = true
	}
	filtered := existing[:0]
	for _, t := range existing {
		if !keySet[t.Key] {
			filtered = append(filtered, t)
		}
	}
	b.tags[resourceARN] = filtered

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return b.tags[resourceARN], nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every WorkMail organization, user, group, and
// resource ARN that currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceARN, tagList := range b.tags {
		if len(tagList) == 0 {
			continue
		}

		m := make(map[string]string, len(tagList))
		for _, t := range tagList {
			m[t.Key] = t.Value
		}

		out = append(out, TaggedEntry{ARN: resourceARN, Tags: m})
	}

	return out
}
