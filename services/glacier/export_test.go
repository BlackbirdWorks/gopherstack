package glacier

// ExportedInitiateJobRequest is an alias for the unexported initiateJobRequest type,
// made available for use in external (_test) test packages.
type ExportedInitiateJobRequest = initiateJobRequest

// ComputeTreeHash exposes the internal tree-hash computation for testing.
func ComputeTreeHash(data []byte) string {
	return computeTreeHash(data)
}

// ValidateDescription exposes description validation for testing.
func ValidateDescription(s string) error {
	return validateDescription(s)
}

// ValidateVaultName exposes vault name validation for testing.
func ValidateVaultName(name string) error {
	return validateVaultName(name)
}

// CsvField exposes the CSV field encoder for testing.
func CsvField(s string) string {
	return csvField(s)
}

// IsValidMultipartRange exposes the multipart Content-Range validator for testing.
func IsValidMultipartRange(rangeHeader string) bool {
	return isValidMultipartRange(rangeHeader)
}

// GetVaultLastInventoryDate returns the LastInventoryDate for the named vault.
func GetVaultLastInventoryDate(b *InMemoryBackend, accountID, region, vaultName string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	v, ok := b.vaults[key]
	if !ok {
		return ""
	}

	return v.LastInventoryDate
}

// GetVaultLockState returns the State field for the named vault's lock.
// Returns "Unlocked" if no lock exists.
func GetVaultLockState(b *InMemoryBackend, accountID, region, vaultName string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}
	lock, ok := b.vaultLocks[key]

	if !ok {
		return "Unlocked"
	}

	return lock.State
}

// SetVaultLockExpired backdates the expiration of an InProgress vault lock so that
// expiry logic can be exercised without real time passing.
func SetVaultLockExpired(b *InMemoryBackend, accountID, region, vaultName string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := vaultKey{AccountID: accountID, Region: region, VaultName: vaultName}

	if lock, ok := b.vaultLocks[key]; ok {
		lock.ExpirationDate = "2000-01-01T00:00:00.000Z"
	}
}

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

// VaultLockCount returns the number of vault locks (for testing only).
func VaultLockCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.vaultLocks)
}

// JobCount returns the total number of jobs across all vaults (for testing only).
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0

	for _, m := range b.jobs {
		total += len(m)
	}

	return total
}
