package awsconfig

// TagResource adds tags to the resource identified by arn.
func (b *InMemoryBackend) TagResource(arn string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.resourceTags[arn]
	// Build a map of existing keys for dedup / update.
	idx := make(map[string]int, len(existing))
	for i, t := range existing {
		idx[t.Key] = i
	}

	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			existing[i].Value = t.Value
		} else {
			idx[t.Key] = len(existing)
			existing = append(existing, t)
		}
	}

	b.resourceTags[arn] = existing

	return nil
}

// UntagResource removes tags from the resource identified by arn.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	remove := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		remove[k] = struct{}{}
	}

	existing := b.resourceTags[arn]
	filtered := existing[:0]

	for _, t := range existing {
		if _, skip := remove[t.Key]; !skip {
			filtered = append(filtered, t)
		}
	}

	b.resourceTags[arn] = filtered

	return nil
}

// ListTagsForResource returns all tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.resourceTags[arn]
	if len(tags) == 0 {
		return []Tag{}
	}

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out
}
