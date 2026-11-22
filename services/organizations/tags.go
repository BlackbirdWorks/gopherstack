package organizations

import (
	"cmp"
	"maps"
	"slices"
	"strings"
)

// maxTagsPerResource is AWS Organizations' documented limit of 50 tags per resource
// (root, OU, account, or policy).
const maxTagsPerResource = 50

// reservedTagKeyPrefix is the case-insensitive prefix AWS reserves for system tags;
// callers may not create or modify tags whose key starts with it.
const reservedTagKeyPrefix = "aws:"

// Tag key/value length bounds, from the TagKey/TagValue shapes in botocore's
// organizations service model (botocore 1.43.56,
// data/organizations/2016-11-28/service-2.json.gz): TagKey {min:1, max:128},
// TagValue {min:0, max:256}.
const (
	minTagKeyLength   = 1
	maxTagKeyLength   = 128
	maxTagValueLength = 256
)

// validateNewTags checks a caller-supplied tag list against AWS Organizations'
// tagging constraints before it is merged onto existing (nil for a resource that
// doesn't exist yet, e.g. a Create* call's Tags parameter). It must be called
// before any state mutation so an invalid tag list leaves the target unmutated,
// matching AWS's documented "the entire request fails" behavior for Tags
// parameters on CreateAccount/CreateOrganizationalUnit/CreatePolicy/TagResource.
func validateNewTags(existing map[string]string, newTags []Tag) error {
	if len(newTags) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(newTags))

	for _, t := range newTags {
		if len(t.Key) < minTagKeyLength || len(t.Key) > maxTagKeyLength {
			return ErrInvalidTagKeyLength
		}

		if len(t.Value) > maxTagValueLength {
			return ErrInvalidTagValueLength
		}

		if strings.HasPrefix(strings.ToLower(t.Key), reservedTagKeyPrefix) {
			return ErrInvalidSystemTags
		}

		if _, dup := seen[t.Key]; dup {
			return ErrDuplicateTagKey
		}

		seen[t.Key] = struct{}{}
	}

	final := make(map[string]struct{}, len(existing)+len(seen))
	for k := range existing {
		final[k] = struct{}{}
	}

	for k := range seen {
		final[k] = struct{}{}
	}

	if len(final) > maxTagsPerResource {
		return ErrTagLimitExceeded
	}

	return nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceID string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return ErrTargetNotFound
	}

	if err := validateNewTags(b.tags[resourceID], tags); err != nil {
		return err
	}

	b.setTagsLocked(resourceID, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceID string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return ErrTargetNotFound
	}

	t := b.tags[resourceID]
	if t == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceID string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.resourceExistsLocked(resourceID) {
		return nil, ErrTargetNotFound
	}

	t := b.tags[resourceID]
	out := make([]Tag, 0, len(t))

	for k, v := range t {
		out = append(out, Tag{Key: k, Value: v})
	}

	slices.SortFunc(out, func(a, b Tag) int { return cmp.Compare(a.Key, b.Key) })

	return out, nil
}

// setTagsLocked merges tags onto a resource. Must be called with lock held.
func (b *InMemoryBackend) setTagsLocked(resourceID string, tags []Tag) {
	if len(tags) == 0 {
		return
	}

	if b.tags[resourceID] == nil {
		b.tags[resourceID] = make(map[string]string)
	}

	for _, t := range tags {
		b.tags[resourceID][t.Key] = t.Value
	}
}

// TaggedEntry pairs a taggable Organizations resource ARN with its tag set, for
// cross-service tagging discovery (see cli.go's wireTaggingOrganizations).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// resourceIDForARNLocked is resourceARNLocked's inverse (same four-kind scope,
// organization excluded): the cross-service tagging aggregator deals only in
// ARNs, but TagResource/UntagResource are keyed by the internal resourceID, so
// tag/untag calls routed through an ARN need this to find it first. Must be
// called with lock held.
func (b *InMemoryBackend) resourceIDForARNLocked(resourceARN string) (string, bool) {
	if b.root != nil && b.root.ARN == resourceARN {
		return b.root.ID, true
	}

	for _, ou := range b.ous.All() {
		if ou.ARN == resourceARN {
			return ou.ID, true
		}
	}

	for _, acct := range b.accounts.All() {
		if acct.ARN == resourceARN {
			return acct.ID, true
		}
	}

	for _, p := range b.policies.All() {
		if p.PolicySummary.ARN == resourceARN {
			return p.PolicySummary.ID, true
		}
	}

	return "", false
}

// TaggedResources lists every root, organizational unit, account and policy that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every root, organizational unit, account and policy, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	add := func(id, arn string) {
		t := b.tags[id]
		if !taggedOnly || len(t) > 0 {
			out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
		}
	}

	if b.root != nil {
		add(b.root.ID, b.root.ARN)
	}

	for _, ou := range b.ous.All() {
		add(ou.ID, ou.ARN)
	}

	for _, acct := range b.accounts.All() {
		add(acct.ID, acct.ARN)
	}

	for _, p := range b.policies.All() {
		add(p.PolicySummary.ID, p.PolicySummary.ARN)
	}

	return out
}

// TagResourceByARN is TagResource's cross-service tagging entry point: it
// resolves resourceARN to the internal resourceID TagResource actually keys on.
func (b *InMemoryBackend) TagResourceByARN(resourceARN string, newTags map[string]string) error {
	b.mu.RLock("TagResourceByARN")
	resourceID, ok := b.resourceIDForARNLocked(resourceARN)
	b.mu.RUnlock()

	if !ok {
		return ErrTargetNotFound
	}

	tagList := make([]Tag, 0, len(newTags))
	for k, v := range newTags {
		tagList = append(tagList, Tag{Key: k, Value: v})
	}

	return b.TagResource(resourceID, tagList)
}

// UntagResourceByARN is UntagResource's cross-service tagging entry point: it
// resolves resourceARN to the internal resourceID UntagResource actually keys on.
func (b *InMemoryBackend) UntagResourceByARN(resourceARN string, tagKeys []string) error {
	b.mu.RLock("UntagResourceByARN")
	resourceID, ok := b.resourceIDForARNLocked(resourceARN)
	b.mu.RUnlock()

	if !ok {
		return ErrTargetNotFound
	}

	return b.UntagResource(resourceID, tagKeys)
}
