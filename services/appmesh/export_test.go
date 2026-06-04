package appmesh

// NewInMemoryBackendForTest exposes NewInMemoryBackend for test packages.
func NewInMemoryBackendForTest(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackend(accountID, region)
}
