package kms

import "time"

// SetTags exposes setTags for testing.
func (h *Handler) SetTags(resourceID string, kv map[string]string) { h.setTags(resourceID, kv) }

// RemoveTags exposes removeTags for testing.
func (h *Handler) RemoveTags(resourceID string, keys []string) { h.removeTags(resourceID, keys) }

// GetTags exposes getTags for testing.
func (h *Handler) GetTags(resourceID string) map[string]string { return h.getTags(resourceID) }

// SetDeletionDateForTest directly sets a key's DeletionDate to the given time.
// Used to simulate elapsed deletion windows without sleeping.
func (b *InMemoryBackend) SetDeletionDateForTest(keyID string, t time.Time) {
	b.mu.Lock("SetDeletionDateForTest")
	defer b.mu.Unlock()

	if key, ok := b.keys[keyID]; ok {
		key.DeletionDate = UnixTimeFloat(t)
	}
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}
