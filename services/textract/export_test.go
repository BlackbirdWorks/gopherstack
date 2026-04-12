package textract

// NewInMemoryBackendWithCap creates a backend with a custom job history cap for testing.
func NewInMemoryBackendWithCap(maxJobs int) *InMemoryBackend {
	b := NewInMemoryBackend()
	b.maxJobs = maxJobs

	return b
}
