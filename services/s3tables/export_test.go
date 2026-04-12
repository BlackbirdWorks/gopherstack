package s3tables

// BucketCount returns the number of table buckets in the backend.
func BucketCount(b *InMemoryBackend) int {
	b.mu.RLock("BucketCount")
	defer b.mu.RUnlock()

	return len(b.tableBuckets)
}

// NamespaceCount returns the number of namespaces in the backend.
func NamespaceCount(b *InMemoryBackend) int {
	b.mu.RLock("NamespaceCount")
	defer b.mu.RUnlock()

	return len(b.namespaces)
}

// TableCount returns the number of tables in the backend.
func TableCount(b *InMemoryBackend) int {
	b.mu.RLock("TableCount")
	defer b.mu.RUnlock()

	return len(b.tables)
}

// HandlerOpsLen returns the number of operations the handler supports.
func HandlerOpsLen(h *Handler) int { return len(h.GetSupportedOperations()) }

// BucketReplicationCount returns the number of bucket replication configs in the backend.
func BucketReplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("BucketReplicationCount")
	defer b.mu.RUnlock()

	return len(b.bucketReplication)
}

// TableReplicationCount returns the number of table replication entries in the backend.
func TableReplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("TableReplicationCount")
	defer b.mu.RUnlock()

	return len(b.tableReplication)
}

// TableRecordExpiryCount returns the number of table record expiry configs in the backend.
func TableRecordExpiryCount(b *InMemoryBackend) int {
	b.mu.RLock("TableRecordExpiryCount")
	defer b.mu.RUnlock()

	return len(b.tableRecordExpiry)
}

// AddBucketReplicationInternal directly inserts a replication config for tests.
func AddBucketReplicationInternal(b *InMemoryBackend, bucketARN string, cfg *BucketReplicationConfig) {
	b.mu.Lock("AddBucketReplicationInternal")
	defer b.mu.Unlock()

	b.bucketReplication[bucketARN] = cfg
}
