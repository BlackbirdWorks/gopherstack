package glue

// GlueResourceNameForTest exposes glueResourceName for unit tests.
func GlueResourceNameForTest(resourceARN, resourceType string) string {
	return glueResourceName(resourceARN, resourceType)
}

// DatabaseCount returns the number of databases stored in the backend. Used only in tests.
func DatabaseCount(b *InMemoryBackend) int {
	b.mu.RLock("DatabaseCount")
	defer b.mu.RUnlock()

	return len(b.databases)
}

// TableCount returns the total number of tables stored in the backend. Used only in tests.
func TableCount(b *InMemoryBackend) int {
	b.mu.RLock("TableCount")
	defer b.mu.RUnlock()

	return len(b.tables)
}

// CrawlerCount returns the number of crawlers stored in the backend. Used only in tests.
func CrawlerCount(b *InMemoryBackend) int {
	b.mu.RLock("CrawlerCount")
	defer b.mu.RUnlock()

	return len(b.crawlers)
}

// JobCount returns the number of jobs stored in the backend. Used only in tests.
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return len(b.jobs)
}

// PartitionCount returns the number of partitions stored in the backend. Used only in tests.
func PartitionCount(b *InMemoryBackend) int {
	b.mu.RLock("PartitionCount")
	defer b.mu.RUnlock()

	return len(b.partitions)
}

// TableVersionCount returns the number of table versions stored in the backend. Used only in tests.
func TableVersionCount(b *InMemoryBackend) int {
	b.mu.RLock("TableVersionCount")
	defer b.mu.RUnlock()

	return len(b.tableVersions)
}

// ConnectionCount returns the number of connections stored in the backend. Used only in tests.
func ConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return len(b.connections)
}

// BlueprintCount returns the number of blueprints stored in the backend. Used only in tests.
func BlueprintCount(b *InMemoryBackend) int {
	b.mu.RLock("BlueprintCount")
	defer b.mu.RUnlock()

	return len(b.blueprints)
}

// CustomEntityTypeCount returns the number of custom entity types in the backend. Used only in tests.
func CustomEntityTypeCount(b *InMemoryBackend) int {
	b.mu.RLock("CustomEntityTypeCount")
	defer b.mu.RUnlock()

	return len(b.customEntityTypes)
}

// DataQualityResultCount returns the number of data quality results in the backend. Used only in tests.
func DataQualityResultCount(b *InMemoryBackend) int {
	b.mu.RLock("DataQualityResultCount")
	defer b.mu.RUnlock()

	return len(b.dataQualityResult)
}

// DevEndpointCount returns the number of dev endpoints in the backend. Used only in tests.
func DevEndpointCount(b *InMemoryBackend) int {
	b.mu.RLock("DevEndpointCount")
	defer b.mu.RUnlock()

	return len(b.devEndpoints)
}

// HandlerOpsLen returns the number of operations registered in the handler's dispatch table.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}
