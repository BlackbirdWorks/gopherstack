package resiliencehub

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TaggedEntry is one tagged resource, used by TaggedResources for the
// resourcegroupstaggingapi integration (cli.go's wireTaggingResilienceHub).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// resolveTaggableLocked resolves resourceArn to the tags.Tags instance of
// whichever resource kind it names. App, ResiliencyPolicy, AppAssessment,
// and RecommendationTemplate all carry a Tags map with their own ARN-keyed
// TagResource/UntagResource/ListTagsForResource surface (confirmed from
// every ARN-bearing field's own doc comment in types/types.go -- see
// PARITY.md's tagging section; CreateRecommendationTemplateInput.Tags at
// resiliencehub@v1.38.3 api_op_CreateRecommendationTemplate.go:46).
func (b *InMemoryBackend) resolveTaggableLocked(resourceArn string) (*tags.Tags, bool) {
	if a, ok := b.resolveAppLocked(resourceArn); ok {
		return a.Tags, true
	}

	if p, ok := b.resolvePolicyLocked(resourceArn); ok {
		return p.Tags, true
	}

	if asmt, ok := b.resolveAssessmentLocked(resourceArn); ok {
		return asmt.Tags, true
	}

	if t, ok := b.resolveTemplateLocked(resourceArn); ok {
		return t.Tags, true
	}

	return nil, false
}

// TagResource adds or updates tags on the App, ResiliencyPolicy, or
// AppAssessment identified by resourceArn.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.Merge(newTags)

	return nil
}

// UntagResource removes tags from the App, ResiliencyPolicy, or
// AppAssessment identified by resourceArn.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns the tags for the App, ResiliencyPolicy, or
// AppAssessment identified by resourceArn.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return nil, notFoundError(resourceTaggable, resourceArn)
	}

	return t.Clone(), nil
}

// TaggedResources lists every app, policy, assessment and template that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every app, policy, assessment and template, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, a := range b.apps.Snapshot() {
		out = appendEntry(out, a.ARN, a.Tags, taggedOnly)
	}

	for _, p := range b.policies.Snapshot() {
		out = appendEntry(out, p.ARN, p.Tags, taggedOnly)
	}

	for _, asmt := range b.assessments.Snapshot() {
		out = appendEntry(out, asmt.ARN, asmt.Tags, taggedOnly)
	}

	for _, t := range b.templates.Snapshot() {
		out = appendEntry(out, t.ARN, t.Tags, taggedOnly)
	}

	return out
}

func appendEntry(out []TaggedEntry, arn string, t *tags.Tags, taggedOnly bool) []TaggedEntry {
	empty := t == nil || t.Len() == 0
	if taggedOnly && empty {
		return out
	}

	entry := TaggedEntry{ARN: arn}
	if !empty {
		entry.Tags = t.Clone()
	}

	return append(out, entry)
}
