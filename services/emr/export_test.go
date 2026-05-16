package emr

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultJanitorInterval

// DefaultTerminatedTTL exposes the package default terminated cluster TTL for testing.
const DefaultTerminatedTTL = defaultTerminatedTTL

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	return h.janitor.TaskTimeout
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
func (h *Handler) GetJanitorInterval() time.Duration {
	return h.janitor.Interval
}

// GetJanitorTerminatedTTL returns the TerminatedTTL configured on the handler's janitor.
func (h *Handler) GetJanitorTerminatedTTL() time.Duration {
	return h.janitor.TerminatedTTL
}

// ClusterCount returns the number of clusters in the backend. Used only in tests.
func (b *InMemoryBackend) ClusterCount() int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// SecurityConfigCount returns the number of security configurations in the backend.
func (b *InMemoryBackend) SecurityConfigCount() int {
	b.mu.RLock("SecurityConfigCount")
	defer b.mu.RUnlock()

	return len(b.securityConfigs)
}

// StudioCount returns the number of studios in the backend.
func (b *InMemoryBackend) StudioCount() int {
	b.mu.RLock("StudioCount")
	defer b.mu.RUnlock()

	return len(b.studios)
}

// PersistentAppUICount returns the number of persistent app UIs in the backend.
func (b *InMemoryBackend) PersistentAppUICount() int {
	b.mu.RLock("PersistentAppUICount")
	defer b.mu.RUnlock()

	return len(b.persistentAppUIs)
}

// StudioSessionMappingCount returns the number of studio session mappings in the backend.
func (b *InMemoryBackend) StudioSessionMappingCount() int {
	b.mu.RLock("StudioSessionMappingCount")
	defer b.mu.RUnlock()

	return len(b.studioSessionMappings)
}

// HandlerOpsLen returns the number of operations in the cached dispatch table.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// DefaultReleaseLabel exposes the default release label for testing.
const DefaultReleaseLabel = defaultReleaseLabel
