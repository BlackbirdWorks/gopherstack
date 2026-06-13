package bedrockagent

// Exported for testing.

func NewTestBackend(region, accountID string) *InMemoryBackend {
	return NewInMemoryBackend(region, accountID)
}

func NewTestHandler(b StorageBackend) *Handler {
	return NewHandler(b)
}
