package sagemaker

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func sumRegions[T any](m map[string]*store.Table[T]) int {
	total := 0
	for _, tbl := range m {
		total += tbl.Len()
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

	b.monitoringExecutionsStore(region).Put(e)
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

// SeedModelCreationTime overwrites a model's CreationTime for
// CreationTimeAfter boundary tests: a wire-level test can't reliably hit
// the exact second boundary since epoch-seconds JSON round-tripping floors
// the resource's true (sub-second) CreationTime before it comes back as a
// filter value, so the two virtually never compare equal without direct
// control here.
func SeedModelCreationTime(b *InMemoryBackend, region, name string, t time.Time) {
	b.mu.Lock("SeedModelCreationTime")
	defer b.mu.Unlock()

	if m, ok := b.modelsStore(region).Get(name); ok {
		m.CreationTime = t
	}
}

// SeedEndpointConfigCreationTime overwrites an endpoint config's
// CreationTime -- see [SeedModelCreationTime].
func SeedEndpointConfigCreationTime(b *InMemoryBackend, region, name string, t time.Time) {
	b.mu.Lock("SeedEndpointConfigCreationTime")
	defer b.mu.Unlock()

	if ec, ok := b.endpointConfigsStore(region).Get(name); ok {
		ec.CreationTime = t
	}
}

// SeedAlgorithmCreationTime overwrites an algorithm's CreationTime -- see
// [SeedModelCreationTime].
func SeedAlgorithmCreationTime(b *InMemoryBackend, region, name string, t time.Time) {
	b.mu.Lock("SeedAlgorithmCreationTime")
	defer b.mu.Unlock()

	if al, ok := b.algorithmsStore(region).Get(name); ok {
		al.CreationTime = t
	}
}

// AssociationTagCount returns the number of tags stored on the association
// between sourceArn and destinationArn, for tests proving AddAssociation's
// Tags field (not a real AddAssociationInput member) is never applied.
func AssociationTagCount(b *InMemoryBackend, region, sourceArn, destinationArn string) int {
	b.mu.RLock("AssociationTagCount")
	defer b.mu.RUnlock()

	a, ok := b.associationsStoreRO(region).Get(associationKey(sourceArn, destinationArn))
	if !ok {
		return -1
	}

	return len(a.Tags)
}
