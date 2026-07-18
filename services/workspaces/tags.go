package workspaces

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	// maxTagsPerResource is the AWS limit for tags per resource.
	maxTagsPerResource = 50
	// maxTagKeyLen is the AWS limit for tag key length.
	maxTagKeyLen = 128
	// maxTagValueLen is the AWS limit for tag value length.
	maxTagValueLen = 256
)

// validateTagEntry checks a single tag key and value for AWS constraints.
func validateTagEntry(key, value string) error {
	if key == "" {
		return awserr.New("tag key must not be empty", awserr.ErrInvalidParameter)
	}

	if len(key) > maxTagKeyLen {
		return awserr.Newf(
			"tag key exceeds maximum length of %d", awserr.ErrInvalidParameter, maxTagKeyLen)
	}

	if len(value) > maxTagValueLen {
		return awserr.Newf(
			"tag value for key %q exceeds maximum length of %d",
			awserr.ErrInvalidParameter, key, maxTagValueLen)
	}

	return nil
}

// CreateTags applies tags to a workspace resource ID.
// Returns InvalidParameterValuesException if tag key/value limits are exceeded
// or if applying the tags would exceed the 50-tag limit per resource.
func (b *InMemoryBackend) CreateTags(resourceID string, tags map[string]string) error {
	for k, v := range tags {
		if err := validateTagEntry(k, v); err != nil {
			return err
		}
	}

	b.mu.Lock("CreateTags")
	defer b.mu.Unlock()

	existing := b.tags[resourceID]
	// Count distinct keys after merge to enforce 50-tag limit.
	newCount := len(existing)

	for k := range tags {
		if _, exists := existing[k]; !exists {
			newCount++
		}
	}

	if newCount > maxTagsPerResource {
		return awserr.Newf(
			"resource %q would exceed maximum tag count of %d",
			awserr.ErrInvalidParameter, resourceID, maxTagsPerResource)
	}

	if b.tags[resourceID] == nil {
		b.tags[resourceID] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceID], tags)

	// Keep workspace tags in sync so DescribeWorkspaces reflects CreateTags changes.
	if w, ok := b.workspaces.Get(resourceID); ok {
		if w.Tags == nil {
			w.Tags = make(map[string]string)
		}

		maps.Copy(w.Tags, tags)
	}

	return nil
}

// DeleteTags removes tags from a workspace resource ID.
func (b *InMemoryBackend) DeleteTags(resourceID string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceID], k)

		if w, ok := b.workspaces.Get(resourceID); ok {
			delete(w.Tags, k)
		}
	}

	return nil
}

// DescribeTags returns tags for a workspace resource ID.
func (b *InMemoryBackend) DescribeTags(resourceID string) (map[string]string, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceID])

	return result, nil
}
