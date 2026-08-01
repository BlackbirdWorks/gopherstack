package outposts

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TaggedEntry is one tagged resource, used by TaggedResources for the
// resourcegroupstaggingapi integration (cli.go's wireTaggingOutposts).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// resolveTaggableLocked resolves resourceArn to the tags.Tags instance of
// whichever resource kind it names. Outposts.Tags and Sites.Tags are the
// only two taggable resource kinds ("Currently, this operation only
// supports tagging ... Outposts" style doc comments plus the fact that
// Site also carries a Tags map with no tag API of its own -- see
// PARITY.md's "families: tagging" note) -- both share this one generic
// ResourceArn-keyed TagResource/UntagResource/ListTagsForResource surface.
func (b *InMemoryBackend) resolveTaggableLocked(resourceArn string) (*tags.Tags, bool) {
	if id, ok := resourceIDFromARN(resourceArn, ":outpost/"); ok {
		if o, outpostOK := b.outposts.Get(id); outpostOK {
			return o.Tags, true
		}

		return nil, false
	}

	if id, ok := resourceIDFromARN(resourceArn, ":site/"); ok {
		if s, siteOK := b.sites.Get(id); siteOK {
			return s.Tags, true
		}

		return nil, false
	}

	return nil, false
}

// TagResource adds or updates tags on the Outpost or Site identified by
// resourceArn.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError("resource", resourceArn)
	}

	t.Merge(newTags)

	return nil
}

// UntagResource removes tags from the Outpost or Site identified by
// resourceArn.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError("resource", resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns the tags for the Outpost or Site identified by
// resourceArn.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return nil, notFoundError("resource", resourceArn)
	}

	return t.Clone(), nil
}

// TaggedResources returns every tagged Outpost and Site, for the
// resourcegroupstaggingapi integration.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, o := range b.outposts.Snapshot() {
		if o.Tags != nil && o.Tags.Len() > 0 {
			out = append(out, TaggedEntry{ARN: o.ARN, Tags: o.Tags.Clone()})
		}
	}

	for _, s := range b.sites.Snapshot() {
		if s.Tags != nil && s.Tags.Len() > 0 {
			out = append(out, TaggedEntry{ARN: s.ARN, Tags: s.Tags.Clone()})
		}
	}

	return out
}
