package s3tables

// BucketCount returns the number of table buckets in the backend.
func BucketCount(b *InMemoryBackend) int {
	b.muBuckets.RLock("BucketCount")
	defer b.muBuckets.RUnlock()

	return b.tableBuckets.Len()
}

// NamespaceCount returns the number of namespaces in the backend.
func NamespaceCount(b *InMemoryBackend) int {
	b.muNamespaces.RLock("NamespaceCount")
	defer b.muNamespaces.RUnlock()

	return b.namespaces.Len()
}

// TableCount returns the number of tables in the backend.
func TableCount(b *InMemoryBackend) int {
	b.muTables.RLock("TableCount")
	defer b.muTables.RUnlock()

	return b.tables.Len()
}

// HandlerOpsLen returns the number of operations the handler supports.
func HandlerOpsLen(h *Handler) int { return len(h.GetSupportedOperations()) }

// BucketReplicationCount returns the number of bucket replication configs in the backend.
func BucketReplicationCount(b *InMemoryBackend) int {
	b.muState.RLock("BucketReplicationCount")
	defer b.muState.RUnlock()

	return b.bucketReplication.Len()
}

// TableReplicationCount returns the number of table replication entries in the backend.
func TableReplicationCount(b *InMemoryBackend) int {
	b.muState.RLock("TableReplicationCount")
	defer b.muState.RUnlock()

	return b.tableReplication.Len()
}

// TableRecordExpiryCount returns the number of table record expiry configs in the backend.
func TableRecordExpiryCount(b *InMemoryBackend) int {
	b.muState.RLock("TableRecordExpiryCount")
	defer b.muState.RUnlock()

	return b.tableRecordExpiry.Len()
}

// AddBucketReplicationInternal directly inserts a replication config for tests.
func AddBucketReplicationInternal(b *InMemoryBackend, bucketARN string, cfg *BucketReplicationConfig) {
	b.muState.Lock("AddBucketReplicationInternal")
	defer b.muState.Unlock()

	cfg.TableBucketARN = bucketARN
	b.bucketReplication.Put(cfg)
}
