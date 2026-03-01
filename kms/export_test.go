package kms

// SetTags exposes setTags for testing.
func (h *Handler) SetTags(resourceID string, kv map[string]string) { h.setTags(resourceID, kv) }

// RemoveTags exposes removeTags for testing.
func (h *Handler) RemoveTags(resourceID string, keys []string) { h.removeTags(resourceID, keys) }

// GetTags exposes getTags for testing.
func (h *Handler) GetTags(resourceID string) map[string]string { return h.getTags(resourceID) }
