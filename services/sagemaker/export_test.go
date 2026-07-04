package sagemaker

func sumRegions[T any](m map[string]map[string]*T) int {
	total := 0
	for _, regionMap := range m {
		total += len(regionMap)
	}

	return total
}

// ModelCount returns the number of models in the backend.
func ModelCount(b *InMemoryBackend) int {
	b.mu.RLock("ModelCount")
	defer b.mu.RUnlock()

	return sumRegions(b.models)
}

// EndpointConfigCount returns the number of endpoint configs in the backend.
func EndpointConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointConfigCount")
	defer b.mu.RUnlock()

	return sumRegions(b.endpointConfigs)
}

// AssociationCount returns the number of associations in the backend.
func AssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("AssociationCount")
	defer b.mu.RUnlock()

	return sumRegions(b.associations)
}

// TrialComponentAssociationCount returns the number of trial component associations in the backend.
func TrialComponentAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("TrialComponentAssociationCount")
	defer b.mu.RUnlock()

	return sumRegions(b.trialComponentAssociations)
}

// ActionCount returns the number of actions in the backend.
func ActionCount(b *InMemoryBackend) int {
	b.mu.RLock("ActionCount")
	defer b.mu.RUnlock()

	return sumRegions(b.actions)
}

// AlgorithmCount returns the number of algorithms in the backend.
func AlgorithmCount(b *InMemoryBackend) int {
	b.mu.RLock("AlgorithmCount")
	defer b.mu.RUnlock()

	return sumRegions(b.algorithms)
}

// ClusterCount returns the number of clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return sumRegions(b.clusters)
}

// ModelPackageCount returns the number of model packages in the backend.
func ModelPackageCount(b *InMemoryBackend) int {
	b.mu.RLock("ModelPackageCount")
	defer b.mu.RUnlock()

	return sumRegions(b.modelPackages)
}

// HandlerOpsLen returns the number of supported operations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// SeedMonitoringExecution inserts a monitoring execution directly into the
// backend for test purposes. AWS provides no CreateMonitoringExecution API —
// executions are produced automatically when a monitoring schedule's
// periodic run completes, which this emulator does not simulate.
func SeedMonitoringExecution(b *InMemoryBackend, region string, e *MonitoringExecution) {
	b.mu.Lock("SeedMonitoringExecution")
	defer b.mu.Unlock()

	store := b.monitoringExecutionsStore(region)
	store[e.MonitoringScheduleName+"|"+e.ProcessingJobArn] = e
}

// SeedMonitoringAlertHistory inserts an alert-history entry directly into the
// backend for test purposes. AWS provides no API to record an alert-status
// transition directly — history entries are produced automatically as
// monitoring executions complete, which this emulator does not simulate.
func SeedMonitoringAlertHistory(b *InMemoryBackend, region string, e *MonitoringAlertHistoryEntry) {
	b.mu.Lock("SeedMonitoringAlertHistory")
	defer b.mu.Unlock()

	b.monitoringAlertHistory[region] = append(b.monitoringAlertHistory[region], e)
}
