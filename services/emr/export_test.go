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

// countNested sums the sizes of all per-region inner maps in a region-nested store.
func countNested[V any](outer map[string]map[string]V) int {
	total := 0
	for _, inner := range outer {
		total += len(inner)
	}

	return total
}

// ClusterCount returns the total number of clusters across all regions. Used only in tests.
func (b *InMemoryBackend) ClusterCount() int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return countNested(b.clusters)
}

// SecurityConfigCount returns the total number of security configurations across all regions.
func (b *InMemoryBackend) SecurityConfigCount() int {
	b.mu.RLock("SecurityConfigCount")
	defer b.mu.RUnlock()

	return countNested(b.securityConfigs)
}

// StudioCount returns the total number of studios across all regions.
func (b *InMemoryBackend) StudioCount() int {
	b.mu.RLock("StudioCount")
	defer b.mu.RUnlock()

	return countNested(b.studios)
}

// PersistentAppUICount returns the total number of persistent app UIs across all regions.
func (b *InMemoryBackend) PersistentAppUICount() int {
	b.mu.RLock("PersistentAppUICount")
	defer b.mu.RUnlock()

	return countNested(b.persistentAppUIs)
}

// StudioSessionMappingCount returns the total number of studio session mappings across all regions.
func (b *InMemoryBackend) StudioSessionMappingCount() int {
	b.mu.RLock("StudioSessionMappingCount")
	defer b.mu.RUnlock()

	return countNested(b.studioSessionMappings)
}

// HandlerOpsLen returns the number of operations in the cached dispatch table.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// DefaultReleaseLabel exposes the default release label for testing.
const DefaultReleaseLabel = defaultReleaseLabel
