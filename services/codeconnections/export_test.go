package codeconnections

import "context"

// CtxRegion returns a context with the given region set. Used only in tests
// (persistence_test.go) that need to exercise multi-region state without
// going through the HTTP handler's X-Amz-Region header parsing.
func CtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

// ConnectionCount returns the number of connections stored in the backend for the default region.
// Used only in tests.
func (b *InMemoryBackend) ConnectionCount() int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return len(b.connectionsByRegion.Get(b.defaultRegion))
}

// HostCount returns the number of hosts stored in the backend for the default region.
// Used only in tests.
func (b *InMemoryBackend) HostCount() int {
	b.mu.RLock("HostCount")
	defer b.mu.RUnlock()

	return len(b.hostsByRegion.Get(b.defaultRegion))
}

// RepositoryLinkCount returns the number of repository links stored in the backend for the
// default region. Used only in tests.
func (b *InMemoryBackend) RepositoryLinkCount() int {
	b.mu.RLock("RepositoryLinkCount")
	defer b.mu.RUnlock()

	return len(b.repositoryLinksByRegion.Get(b.defaultRegion))
}

// SyncConfigurationCount returns the number of sync configurations stored in the backend for
// the default region. Used only in tests.
func (b *InMemoryBackend) SyncConfigurationCount() int {
	b.mu.RLock("SyncConfigurationCount")
	defer b.mu.RUnlock()

	return len(b.syncConfigurationsByRegion.Get(b.defaultRegion))
}
