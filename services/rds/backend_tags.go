package rds

// AddTagsToResource adds or overwrites tags on the resource identified by arn.
func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	current := b.tags[arn]
	// Build an index for O(1) key lookup.
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}

	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}

	b.tags[arn] = current
}

// RemoveTagsFromResource removes the named tags from the resource identified by arn.
func (b *InMemoryBackend) RemoveTagsFromResource(arn string, keys []string) {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}

	current := b.tags[arn]
	kept := make([]Tag, 0, len(current))

	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}

	b.tags[arn] = kept
}

// ListTagsForResource returns the tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp
}
