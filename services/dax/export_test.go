package dax

// PaginateClustersForTest exposes the unexported InMemoryBackend.paginateClusters
// pagination helper so its arithmetic can be verified directly, independent
// of DescribeClusters' locking/sorting/filtering.
func PaginateClustersForTest(
	b *InMemoryBackend, all []*Cluster, maxResults int, nextToken string,
) ([]*Cluster, string) {
	return b.paginateClusters(all, maxResults, nextToken)
}

// PaginateParametersForTest exposes the unexported paginateParameters
// pagination helper so its arithmetic can be verified directly.
func PaginateParametersForTest(all []*Parameter, maxResults int, nextToken string) ([]*Parameter, string) {
	return paginateParameters(all, maxResults, nextToken)
}

// PaginateListStringsForTest exposes the unexported generic paginateList
// pagination helper (instantiated for strings) so its arithmetic can be
// verified directly.
func PaginateListStringsForTest(all []string, maxResults int, nextToken string) ([]string, string) {
	identity := func(s string) string { return s }

	return paginateList(all, maxResults, nextToken, identity, identity)
}

// EmitEventForTest appends an event to the backend's ring buffer under the
// write lock, exposing the unexported emitEventLocked for pagination tests.
func EmitEventForTest(b *InMemoryBackend, sourceName, sourceType, message string) {
	b.mu.Lock("EmitEventForTest")
	defer b.mu.Unlock()

	b.emitEventLocked(sourceName, sourceType, message)
}

func SetClusterAvailableForTest(b *InMemoryBackend, name string) {
	b.mu.Lock("SetClusterAvailableForTest")
	defer b.mu.Unlock()
	if c, ok := b.clusters.Get(name); ok {
		c.Status = StatusAvailable
	}
}
