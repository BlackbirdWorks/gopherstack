package organizations

import (
	"cmp"
	"slices"
	"strings"
)

// maxTagsPerResource is AWS Organizations' documented limit of 50 tags per resource
// (root, OU, account, or policy).
const maxTagsPerResource = 50

// reservedTagKeyPrefix is the case-insensitive prefix AWS reserves for system tags;
// callers may not create or modify tags whose key starts with it.
const reservedTagKeyPrefix = "aws:"

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
