package bedrockagent

// Exported for testing.

func NewTestBackend(region, accountID string) *InMemoryBackend {
	return NewInMemoryBackend(region, accountID)
}

func NewTestHandler(b StorageBackend) *Handler {
	return NewHandler(b)
}

// PaginateForTest exposes the unexported paginate helper for direct
// arithmetic testing (pagination_arithmetic_test.go).
func PaginateForTest(ids []string, nextToken string, maxResults int) ([]string, string) {
	return paginate(ids, nextToken, maxResults)
}
