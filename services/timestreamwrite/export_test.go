package timestreamwrite

// DatabaseCount returns the number of databases stored in the backend.
// Exposed for testing only.
func DatabaseCount(b *InMemoryBackend) int {
	b.mu.RLock("DatabaseCount")
	defer b.mu.RUnlock()

	return b.databases.Len()
}

// TableCount returns the total number of tables across all databases.
// Exposed for testing only.
func TableCount(b *InMemoryBackend) int {
	b.mu.RLock("TableCount")
	defer b.mu.RUnlock()

	return b.tables.Len()
}

// BatchLoadTaskCount returns the number of batch load tasks stored in the backend.
// Exposed for testing only.
func BatchLoadTaskCount(b *InMemoryBackend) int {
	b.mu.RLock("BatchLoadTaskCount")
	defer b.mu.RUnlock()

	return b.batchLoadTasks.Len()
}

// TagCount returns the total number of tagged ARNs stored in the backend.
// Exposed for testing only.
func TagCount(b *InMemoryBackend) int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	return len(b.tags)
}

// HandlerOpsLen returns the number of operations registered in the handler's
// dispatch table. Exposed for testing only.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// TableMutexCount returns the number of per-table records slots (each owning a
// mutex) stored in the backend. Exposed for testing only.
func TableMutexCount(b *InMemoryBackend) int {
	b.mu.RLock("TableMutexCount")
	defer b.mu.RUnlock()

	total := 0
	for _, slots := range b.records {
		total += len(slots)
	}

	return total
}

// RecordCount returns the number of records stored for a specific table.
// Exposed for testing only.
func RecordCount(b *InMemoryBackend, dbName, tblName string) int {
	b.mu.RLock("RecordCount")
	defer b.mu.RUnlock()

	slot := b.records[dbName][tblName]
	if slot == nil {
		return 0
	}

	slot.mu.RLock("RecordCount")
	defer slot.mu.RUnlock()

	return len(slot.records)
}
