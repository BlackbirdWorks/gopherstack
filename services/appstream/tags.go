package appstream

import "maps"

// TagResource applies tags to an AppStream resource ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(arn) {
		return ErrNotFound
	}

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tags from an AppStream resource ARN.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(arn) {
		return ErrNotFound
	}

	for _, k := range keys {
		delete(b.tags[arn], k)
	}

	return nil
}

// ListTagsForResource returns tags for an AppStream resource ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownARN(arn) {
		return nil, ErrNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[arn])

	return result, nil
}

// isKnownARN returns true if the ARN corresponds to a known resource.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownARN(arn string) bool {
	_, ok := b.tags[arn]

	return ok
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every fleet, stack, application, app block, app block builder, image builder and
// image plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for key, t := range b.tags {
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
	out := make(
		[]string,
		0,
		b.fleets.Len()+b.stacks.Len()+b.applications.Len()+b.appBlocks.Len()+b.appBlockBuilders.Len()+
			b.imageBuilders.Len()+b.images.Len(),
	)

	for _, f := range b.fleets.All() {
		out = append(out, f.Arn)
	}

	for _, s := range b.stacks.All() {
		out = append(out, s.Arn)
	}

	for _, a := range b.applications.All() {
		out = append(out, a.Arn)
	}

	for _, a := range b.appBlocks.All() {
		out = append(out, a.Arn)
	}

	for _, a := range b.appBlockBuilders.All() {
		out = append(out, a.Arn)
	}

	for _, i := range b.imageBuilders.All() {
		out = append(out, i.Arn)
	}

	for _, i := range b.images.All() {
		out = append(out, i.Arn)
	}

	return out
}
