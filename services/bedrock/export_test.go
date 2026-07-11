package bedrock

import (
	"encoding/json"
	"strconv"
)

// AppendFoundationModelsForTest appends additional foundation models to the backend.
// This is only used in tests to populate beyond the default seeded models.
func (b *InMemoryBackend) AppendFoundationModelsForTest(models []*FoundationModelSummary) {
	b.mu.Lock("AppendFoundationModelsForTest")
	defer b.mu.Unlock()
	b.foundationModels = append(b.foundationModels, models...)
}

// AddBuildWorkflowForTest adds a build workflow to the backend for testing.
func (b *InMemoryBackend) AddBuildWorkflowForTest(policyARN string) *AutomatedReasoningPolicyBuildWorkflow {
	b.mu.Lock("AddBuildWorkflowForTest")
	defer b.mu.Unlock()

	b.arpWorkflowCounter++
	id := "bw-" + strconv.Itoa(b.arpWorkflowCounter)

	wf := &AutomatedReasoningPolicyBuildWorkflow{
		BuildWorkflowID: id,
		PolicyArn:       policyARN,
		Status:          "Running",
	}
	b.arpBuildWorkflows.Put(wf)

	return wf
}

// SnapshotTablesForTest returns a snapshot of every store.Table registered on
// b.registry, keyed by table name (including per-parent lazy tables such as
// "flowVersions:<flowID>"). It is a test-only bridge to the unexported
// registry so blackbox tests can exercise a full Snapshot->Restore round trip
// of the Phase 3.3 pkgs/store conversion; bedrock has no persistence.go of
// its own (see store_setup.go), so there is no production Snapshot to call
// through instead.
func (b *InMemoryBackend) SnapshotTablesForTest() (map[string]json.RawMessage, error) {
	b.mu.RLock("SnapshotTablesForTest")
	defer b.mu.RUnlock()

	return b.registry.SnapshotAll()
}

// RestoreTablesForTest replaces every store.Table registered on b.registry
// with the contents of data, as produced by SnapshotTablesForTest.
func (b *InMemoryBackend) RestoreTablesForTest(data map[string]json.RawMessage) error {
	b.mu.Lock("RestoreTablesForTest")
	defer b.mu.Unlock()

	return b.registry.RestoreAll(data)
}

// ResetTablesForTest clears every store.Table registered on b.registry in
// place, leaving secondary-index maps (guardrailsByName, etc.) and counters
// untouched. Mirrors the registry.ResetAll call inside Reset.
func (b *InMemoryBackend) ResetTablesForTest() {
	b.mu.Lock("ResetTablesForTest")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}
