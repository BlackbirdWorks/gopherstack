package codestarconnections

// ConnectionCount returns the number of connections stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) ConnectionCount() int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return len(b.connections)
}

// HostCount returns the number of hosts stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) HostCount() int {
	b.mu.RLock("HostCount")
	defer b.mu.RUnlock()

	return len(b.hosts)
}

// RepositoryLinkCount returns the number of repository links stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) RepositoryLinkCount() int {
	b.mu.RLock("RepositoryLinkCount")
	defer b.mu.RUnlock()

	return len(b.repositoryLinks)
}

// SyncConfigurationCount returns the number of sync configurations stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) SyncConfigurationCount() int {
	b.mu.RLock("SyncConfigurationCount")
	defer b.mu.RUnlock()

	return len(b.syncConfigurations)
}
