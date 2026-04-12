package sagemaker

// ModelCount returns the number of models in the backend.
func ModelCount(b *InMemoryBackend) int {
	b.mu.RLock("ModelCount")
	defer b.mu.RUnlock()

	return len(b.models)
}

// EndpointConfigCount returns the number of endpoint configs in the backend.
func EndpointConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointConfigCount")
	defer b.mu.RUnlock()

	return len(b.endpointConfigs)
}

// AssociationCount returns the number of associations in the backend.
func AssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("AssociationCount")
	defer b.mu.RUnlock()

	return len(b.associations)
}

// TrialComponentAssociationCount returns the number of trial component associations in the backend.
func TrialComponentAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("TrialComponentAssociationCount")
	defer b.mu.RUnlock()

	return len(b.trialComponentAssociations)
}

// ActionCount returns the number of actions in the backend.
func ActionCount(b *InMemoryBackend) int {
	b.mu.RLock("ActionCount")
	defer b.mu.RUnlock()

	return len(b.actions)
}

// AlgorithmCount returns the number of algorithms in the backend.
func AlgorithmCount(b *InMemoryBackend) int {
	b.mu.RLock("AlgorithmCount")
	defer b.mu.RUnlock()

	return len(b.algorithms)
}

// ClusterCount returns the number of clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// ModelPackageCount returns the number of model packages in the backend.
func ModelPackageCount(b *InMemoryBackend) int {
	b.mu.RLock("ModelPackageCount")
	defer b.mu.RUnlock()

	return len(b.modelPackages)
}

// HandlerOpsLen returns the number of supported operations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
