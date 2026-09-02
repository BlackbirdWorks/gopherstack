package omics

// ReferenceStoreCount returns the number of reference stores in the backend.
func ReferenceStoreCount(b *InMemoryBackend) int {
	b.mu.RLock("ReferenceStoreCount")
	defer b.mu.RUnlock()

	return b.referenceStores.Len()
}

// SequenceStoreCount returns the number of sequence stores in the backend.
func SequenceStoreCount(b *InMemoryBackend) int {
	b.mu.RLock("SequenceStoreCount")
	defer b.mu.RUnlock()

	return b.sequenceStores.Len()
}

// WorkflowCount returns the number of workflows in the backend.
func WorkflowCount(b *InMemoryBackend) int {
	b.mu.RLock("WorkflowCount")
	defer b.mu.RUnlock()

	return b.workflows.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ClassifyPathForTest exposes the unexported classifyPath router entry point
// to omics_test's white-box route-table regression test.
func ClassifyPathForTest(method, path string) string {
	return classifyPath(method, path)
}

// OpDispatchKeysForTest returns every operation name opDispatch has a
// handler entry for.
func OpDispatchKeysForTest() []string {
	table := opDispatch()
	keys := make([]string, 0, len(table))

	for k := range table {
		keys = append(keys, k)
	}

	return keys
}

// OpUnknownForTest exposes the unexported opUnknown sentinel.
func OpUnknownForTest() string { return opUnknown }

// PaginateStringsForTest exposes the unexported paginateStrings pagination
// helper so its arithmetic can be verified directly, independent of any
// particular List* operation built on top of it.
func PaginateStringsForTest(ids []string, nextToken string, maxResults int) ([]string, string) {
	return paginateStrings(ids, nextToken, maxResults)
}

// SetRunBatchStatusForTest force-sets a RunBatch's status, bypassing the
// normal StartRunBatch/CancelRunBatch transitions. This backend completes
// batches synchronously (no async orchestration to drive them through
// CREATING/PENDING/SUBMITTING/INPROGRESS), so exercising DeleteBatch's
// terminal-state precondition against a genuinely non-terminal status
// requires reaching into backend state directly the way a real, slower AWS
// backend would organically pass through it.
func SetRunBatchStatusForTest(b *InMemoryBackend, id, status string) {
	b.mu.Lock("SetRunBatchStatusForTest")
	defer b.mu.Unlock()

	if rb, ok := b.runBatches.Get(id); ok {
		rb.Status = status
	}
}
