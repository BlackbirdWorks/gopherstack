package qldb

// LedgerCount returns the number of ledgers in the backend.
func LedgerCount(b *InMemoryBackend) int {
	b.mu.RLock("LedgerCount")
	defer b.mu.RUnlock()

	return len(b.ledgers)
}

// StreamCount returns the total number of journal kinesis streams across all ledgers.
func StreamCount(b *InMemoryBackend) int {
	b.mu.RLock("StreamCount")
	defer b.mu.RUnlock()

	total := 0
	for _, streams := range b.journalStreams {
		total += len(streams)
	}

	return total
}

// ExportCount returns the number of journal S3 export jobs in the backend.
func ExportCount(b *InMemoryBackend) int {
	b.mu.RLock("ExportCount")
	defer b.mu.RUnlock()

	return len(b.s3Exports)
}

// HandlerOpsLen returns the number of supported operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// AddLedgerInternal inserts a ledger directly into the backend, bypassing
// validation. Useful for seeding test state.
func AddLedgerInternal(b *InMemoryBackend, l *Ledger) {
	b.mu.Lock("AddLedgerInternal")
	defer b.mu.Unlock()

	if l.Tags == nil {
		l.Tags = make(map[string]string)
	}

	b.ledgers[l.Name] = l
	b.ledgerARNIndex[l.ARN] = l.Name
}
