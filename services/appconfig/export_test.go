package appconfig

// ApplicationCount returns the number of applications in the backend. Used only in tests.
func (b *InMemoryBackend) ApplicationCount() int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return len(b.applications)
}

// ApplicationByNameCount returns the number of entries in the name index. Used only in tests.
func (b *InMemoryBackend) ApplicationByNameCount() int {
	b.mu.RLock("ApplicationByNameCount")
	defer b.mu.RUnlock()

	return len(b.applicationsByName)
}

// ExtensionCount returns the number of extensions in the backend. Used only in tests.
func (b *InMemoryBackend) ExtensionCount() int {
	b.mu.RLock("ExtensionCount")
	defer b.mu.RUnlock()

	return len(b.extensions)
}

// ExtensionByNameCount returns the number of entries in the extension name index. Used only in tests.
func (b *InMemoryBackend) ExtensionByNameCount() int {
	b.mu.RLock("ExtensionByNameCount")
	defer b.mu.RUnlock()

	return len(b.extensionsByName)
}

// DeploymentStrategyCount returns the number of deployment strategies. Used only in tests.
func (b *InMemoryBackend) DeploymentStrategyCount() int {
	b.mu.RLock("DeploymentStrategyCount")
	defer b.mu.RUnlock()

	return len(b.deploymentStrategies)
}

// DeploymentStrategyByNameCount returns the number of entries in the deployment strategy name index. Used only in tests.
func (b *InMemoryBackend) DeploymentStrategyByNameCount() int {
	b.mu.RLock("DeploymentStrategyByNameCount")
	defer b.mu.RUnlock()

	return len(b.deploymentStrategiesByName)
}
