package lightsail

// SeedOperationForTest inserts op directly into the backend, bypassing
// newOperationsLocked's time.Now() CreatedAt stamp so tests can construct an
// exact tie between two operations' CreatedAt.
func (b *InMemoryBackend) SeedOperationForTest(op *Operation) {
	b.mu.Lock("SeedOperationForTest")
	defer b.mu.Unlock()
	b.operations.Put(op)
}

// SeedSetupHistoryEntryForTest inserts e directly into the backend,
// bypassing its normal time.Now() CreatedAt stamp.
func (b *InMemoryBackend) SeedSetupHistoryEntryForTest(e *SetupHistoryEntry) {
	b.mu.Lock("SeedSetupHistoryEntryForTest")
	defer b.mu.Unlock()
	b.setupHistory.Put(e)
}
