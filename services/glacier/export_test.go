package glacier

// ExportedInitiateJobRequest is an alias for the unexported initiateJobRequest type,
// made available for use in external (_test) test packages.
type ExportedInitiateJobRequest = initiateJobRequest

// VaultCount returns the number of vaults in the backend (for testing only).
func VaultCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.vaults)
}

// ArchiveCount returns the total number of archives across all vaults (for testing only).
func ArchiveCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0

	for _, m := range b.archives {
		total += len(m)
	}

	return total
}

// MultipartUploadCount returns the total number of in-progress multipart uploads (for testing only).
func MultipartUploadCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0

	for _, m := range b.multipartUploads {
		total += len(m)
	}

	return total
}

// ProvisionedCapacityCount returns the total number of provisioned capacity units (for testing only).
func ProvisionedCapacityCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0

	for _, caps := range b.provisionedCapacity {
		total += len(caps)
	}

	return total
}
