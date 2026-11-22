package accessanalyzer

import "maps"

// findAnalyzerByARNLocked returns the analyzer with the given ARN, if any.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) findAnalyzerByARNLocked(resourceARN string) *Analyzer {
	var found *Analyzer

	b.analyzers.Range(func(a *Analyzer) bool {
		if a.Arn == resourceARN {
			found = a

			return false
		}

		return true
	})

	return found
}

// TagResource sets tags on a resource by ARN. It also mirrors the change
// onto the resource's own Tags field (when the ARN identifies an analyzer),
// which GetAnalyzer/ListAnalyzers render -- a separate surface from the
// ARN-keyed b.tags map ListTagsForResource reads.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], kv)

	if a := b.findAnalyzerByARNLocked(resourceARN); a != nil {
		if a.Tags == nil {
			a.Tags = make(map[string]string)
		}

		maps.Copy(a.Tags, kv)
	}

	return nil
}

// UntagResource removes tags from a resource by ARN, mirroring the removal
// onto the resource's own Tags field (see TagResource).
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	if a := b.findAnalyzerByARNLocked(resourceARN); a != nil {
		for _, k := range tagKeys {
			delete(a.Tags, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return cloneTags(b.tags[resourceARN]), nil
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

// InventoryResources lists every analyzer plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for arn, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[arn] = struct{}{}
		out = append(out, TaggedEntry{ARN: arn, Tags: cloneTags(t)})
	}

	if withUntagged {
		for _, arn := range b.resourceARNsLocked() {
			if _, ok := seen[arn]; !ok {
				seen[arn] = struct{}{}
				out = append(out, TaggedEntry{ARN: arn})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make([]string, 0, b.analyzers.Len())

	for _, a := range b.analyzers.All() {
		out = append(out, a.Arn)
	}

	return out
}
