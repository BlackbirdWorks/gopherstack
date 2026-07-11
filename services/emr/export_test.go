package emr

import (
	"context"
	"time"
)

// WithRegionForTest returns a context carrying region as the per-request AWS
// region, the same way getRegion (backend.go) resolves it from a real
// request. Used only in tests that need to exercise more than one region
// from the emr_test (external) package, which cannot see regionContextKey.
func WithRegionForTest(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, regionContextKey{}, region)
}

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

// ClusterCount returns the total number of clusters across all regions. Used only in tests.
func (b *InMemoryBackend) ClusterCount() int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return b.clusters.Len()
}

// SecurityConfigCount returns the total number of security configurations across all regions.
func (b *InMemoryBackend) SecurityConfigCount() int {
	b.mu.RLock("SecurityConfigCount")
	defer b.mu.RUnlock()

	return b.securityConfigs.Len()
}

// StudioCount returns the total number of studios across all regions.
func (b *InMemoryBackend) StudioCount() int {
	b.mu.RLock("StudioCount")
	defer b.mu.RUnlock()

	return b.studios.Len()
}

// PersistentAppUICount returns the total number of persistent app UIs across all regions.
func (b *InMemoryBackend) PersistentAppUICount() int {
	b.mu.RLock("PersistentAppUICount")
	defer b.mu.RUnlock()

	return b.persistentAppUIs.Len()
}

// StudioSessionMappingCount returns the total number of studio session mappings across all regions.
func (b *InMemoryBackend) StudioSessionMappingCount() int {
	b.mu.RLock("StudioSessionMappingCount")
	defer b.mu.RUnlock()

	return b.studioSessionMappings.Len()
}

// HandlerOpsLen returns the number of operations in the cached dispatch table.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// DefaultReleaseLabel exposes the default release label for testing.
const DefaultReleaseLabel = defaultReleaseLabel

// ListAllClusters returns all clusters in the default region. Used only in tests.
func (b *InMemoryBackend) ListAllClusters() []*Cluster {
	b.mu.RLock("ListAllClusters")
	defer b.mu.RUnlock()

	return b.clustersInRegion(b.region)
}
