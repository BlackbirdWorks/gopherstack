package cloudtrail

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// AddTags adds tags to a resource by ARN or ID.
func (b *InMemoryBackend) AddTags(resourceID string, kv map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceID)
	if err != nil {
		return err
	}
	t.Merge(kv)

	return nil
}

// RemoveTags removes tags from a resource by ARN or ID.
func (b *InMemoryBackend) RemoveTags(resourceID string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceID)
	if err != nil {
		return err
	}
	t.DeleteKeys(keys)

	return nil
}

// ListTags returns tags for the given resource ARNs or IDs.
func (b *InMemoryBackend) ListTags(resourceIDs []string) map[string]map[string]string {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	result := make(map[string]map[string]string, len(resourceIDs))
	for _, rid := range resourceIDs {
		t, err := b.findResourceTagsLocked(rid)
		if err == nil {
			result[rid] = t.Clone()
		}
	}

	return result
}

// findResourceTagsLocked returns the tags.Tags for any supported resource type.
// It must be called with at least a read lock held.
func (b *InMemoryBackend) findResourceTagsLocked(resourceID string) (*tags.Tags, error) {
	if t := b.findByNameOrARNLocked(resourceID); t != nil {
		return t.Tags, nil
	}

	if ch := b.findChannelLocked(resourceID); ch != nil {
		return ch.Tags, nil
	}

	if d := b.findDashboardLocked(resourceID); d != nil {
		return d.Tags, nil
	}

	if eds := b.findEventDataStoreLocked(resourceID); eds != nil {
		return eds.Tags, nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceID)
}
