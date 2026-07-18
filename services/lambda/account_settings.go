package lambda

// --- Account settings ---

// accountDefaultCodeSizeZipped is the default Lambda zip package size limit (50 MB).
const accountDefaultCodeSizeZipped = 50 * 1024 * 1024

// accountDefaultCodeSizeUnzipped is the default Lambda unzipped package size limit (250 MB).
const accountDefaultCodeSizeUnzipped = 250 * 1024 * 1024

// accountDefaultTotalCodeSize is the default Lambda total code storage limit (75 GB).
const accountDefaultTotalCodeSize = 75 * 1024 * 1024 * 1024

// accountDefaultConcurrentExecutions is the default Lambda concurrent execution limit.
const accountDefaultConcurrentExecutions = 1000

// GetAccountSettings returns the Lambda account settings for this in-memory backend.
func (b *InMemoryBackend) GetAccountSettings() *AccountSettingsOutput {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	fnCount := b.functions.Len()
	totalCodeSize := int64(0)

	for _, fn := range b.functions.All() {
		totalCodeSize += fn.CodeSize
	}

	// Compute unreserved concurrency: subtract sum of all per-function reserved values.
	totalReserved := 0
	for _, reserved := range b.functionConcurrencies {
		totalReserved += reserved
	}
	unreserved := max(0, accountDefaultConcurrentExecutions-totalReserved)

	return &AccountSettingsOutput{
		AccountLimit: &AccountLimit{
			CodeSizeUnzipped:               accountDefaultCodeSizeUnzipped,
			CodeSizeZipped:                 accountDefaultCodeSizeZipped,
			ConcurrentExecutions:           accountDefaultConcurrentExecutions,
			TotalCodeSize:                  accountDefaultTotalCodeSize,
			UnreservedConcurrentExecutions: unreserved,
		},
		AccountUsage: &AccountUsage{
			FunctionCount: fnCount,
			TotalCodeSize: totalCodeSize,
		},
	}
}
