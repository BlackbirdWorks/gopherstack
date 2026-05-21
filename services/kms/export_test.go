package kms

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultKMSJanitorInterval

// SetTags exposes setTags for testing.
func (h *Handler) SetTags(resourceID string, kv map[string]string) { h.setTags(resourceID, kv) }

// RemoveTags exposes removeTags for testing.
func (h *Handler) RemoveTags(resourceID string, keys []string) { h.removeTags(resourceID, keys) }

// GetTags exposes getTags for testing.
func (h *Handler) GetTags(resourceID string) map[string]string { return h.getTags(resourceID) }

// HandlerOpsLen returns the number of pre-built dispatch operations.
func HandlerOpsLen(h *Handler) int {
	return len(h.actions)
}

// KeyCount returns the number of keys stored in the backend.
func KeyCount(b *InMemoryBackend) int {
	b.mu.RLock("KeyCount")
	defer b.mu.RUnlock()

	return len(b.keys)
}

// AliasCount returns the number of aliases stored in the backend.
func AliasCount(b *InMemoryBackend) int {
	b.mu.RLock("AliasCount")
	defer b.mu.RUnlock()

	return len(b.aliases)
}

// GrantCount returns the number of grants stored in the backend.
func GrantCount(b *InMemoryBackend) int {
	b.mu.RLock("GrantCount")
	defer b.mu.RUnlock()

	return len(b.grants)
}

// CustomKeyStoreCount returns the number of custom key stores in the backend.
func CustomKeyStoreCount(b *InMemoryBackend) int {
	b.mu.RLock("CustomKeyStoreCount")
	defer b.mu.RUnlock()

	return len(b.customKeyStores)
}

// SetDeletionDateForTest directly sets a key's DeletionDate to the given time.
// Used to simulate elapsed deletion windows without sleeping.
func (b *InMemoryBackend) SetDeletionDateForTest(keyID string, t time.Time) {
	b.mu.Lock("SetDeletionDateForTest")
	defer b.mu.Unlock()

	if key, ok := b.keys[keyID]; ok {
		key.DeletionDate = UnixTimeFloat(t)
	}
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// ScheduleJanitorExpiry pushes an expiry entry into the janitor's heap for testing.
func (j *Janitor) ScheduleJanitorExpiry(keyID string, fireAt float64, isDeletion bool) {
	kind := expiryKindMaterial
	if isDeletion {
		kind = expiryKindDeletion
	}

	j.scheduleExpiry(keyID, fireAt, kind)
}

// GrantTokenTTL exposes grantTokenTTL for testing.
const GrantTokenTTL = grantTokenTTL

// SetGrantTokenIssuedAt directly sets a grant's TokenIssuedAt for expiry testing.
func (b *InMemoryBackend) SetGrantTokenIssuedAt(grantID string, t time.Time) {
	b.mu.Lock("SetGrantTokenIssuedAt")
	defer b.mu.Unlock()

	if g, ok := b.grants[grantID]; ok {
		g.TokenIssuedAt = t
	}
}
