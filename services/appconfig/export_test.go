package appconfig

// ApplicationCount returns the number of applications in the backend. Used only in tests.
func (b *InMemoryBackend) ApplicationCount() int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return b.applications.Len()
}

// ApplicationByNameCount returns the number of entries in the name index. Used only in tests.
func (b *InMemoryBackend) ApplicationByNameCount() int {
	b.mu.RLock("ApplicationByNameCount")
	defer b.mu.RUnlock()

	return b.applicationsByName.Len()
}

// ExtensionCount returns the number of extensions in the backend. Used only in tests.
func (b *InMemoryBackend) ExtensionCount() int {
	b.mu.RLock("ExtensionCount")
	defer b.mu.RUnlock()

	return b.extensions.Len()
}

// ExtensionByNameCount returns the number of entries in the extension name index. Used only in tests.
func (b *InMemoryBackend) ExtensionByNameCount() int {
	b.mu.RLock("ExtensionByNameCount")
	defer b.mu.RUnlock()

	return b.extensionsByName.Len()
}

// DeploymentStrategyCount returns the number of deployment strategies. Used only in tests.
func (b *InMemoryBackend) DeploymentStrategyCount() int {
	b.mu.RLock("DeploymentStrategyCount")
	defer b.mu.RUnlock()

	return b.deploymentStrategies.Len()
}

// DeploymentStrategyByNameCount returns the count of deployment strategy name index entries.
func (b *InMemoryBackend) DeploymentStrategyByNameCount() int {
	b.mu.RLock("DeploymentStrategyByNameCount")
	defer b.mu.RUnlock()

	return b.deploymentStrategiesByName.Len()
}

// DeployedConfigCount returns the number of tracked deployed-configuration
// entries. Used only in tests to verify cascade cleanup on delete.
func (b *InMemoryBackend) DeployedConfigCount() int {
	b.mu.RLock("DeployedConfigCount")
	defer b.mu.RUnlock()

	return len(b.deployedConfigs)
}

// ExtensionAssociationCount returns the number of extension associations in
// the backend. Used only in tests to verify cascade cleanup on delete.
func (b *InMemoryBackend) ExtensionAssociationCount() int {
	b.mu.RLock("ExtensionAssociationCount")
	defer b.mu.RUnlock()

	return b.extensionAssociations.Len()
}

// DeploymentTimerCount returns the number of in-flight deployment
// progression timers currently tracked. Used only in tests to verify the
// background reconciler goroutine drains this map (and, transitively,
// terminates itself) once every deployment reaches a terminal state --
// rather than leaking timers or leaving the goroutine running forever.
func (b *InMemoryBackend) DeploymentTimerCount() int {
	b.mu.RLock("DeploymentTimerCount")
	defer b.mu.RUnlock()

	return len(b.deploymentTimers)
}
