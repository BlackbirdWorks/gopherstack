package textract

import "time"

// NewInMemoryBackendWithCap creates a backend with a custom job history cap for testing.
func NewInMemoryBackendWithCap(maxJobs int) *InMemoryBackend {
	b := NewInMemoryBackend("123456789012", "us-east-1")
	b.maxJobs = maxJobs

	return b
}

// NewInMemoryBackendSync creates a backend where async jobs complete synchronously (zero delay).
// Use this for tests that need immediate SUCCEEDED status without time.Sleep.
func NewInMemoryBackendSync(accountID, region string) *InMemoryBackend {
	b := NewInMemoryBackend(accountID, region)
	b.asyncJobDelay = 0

	return b
}

// SetBackendAsyncDelay sets the async job completion delay on a specific backend.
func SetBackendAsyncDelay(b *InMemoryBackend, d time.Duration) {
	b.asyncJobDelay = d
}

func sumNested[V any](m map[string]map[string]V) int {
	total := 0

	for _, inner := range m {
		total += len(inner)
	}

	return total
}

// JobCount returns the number of document jobs stored in the backend (for testing).
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	return sumNested(b.jobs)
}

// ExpenseJobCount returns the number of expense jobs stored in the backend (for testing).
func ExpenseJobCount(b *InMemoryBackend) int {
	b.mu.RLock("GetExpenseAnalysis")
	defer b.mu.RUnlock()

	return sumNested(b.expenseJobs)
}

// LendingJobCount returns the number of lending jobs stored in the backend (for testing).
func LendingJobCount(b *InMemoryBackend) int {
	b.mu.RLock("GetLendingAnalysis")
	defer b.mu.RUnlock()

	return sumNested(b.lendingJobs)
}

// AdapterCount returns the number of adapters stored in the backend (for testing).
func AdapterCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapter")
	defer b.mu.RUnlock()

	return sumNested(b.adapters)
}

// AdapterVersionCount returns the number of adapter versions stored in the backend (for testing).
func AdapterVersionCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapterVersion")
	defer b.mu.RUnlock()

	return sumNested(b.adapterVersions)
}

// HandlerOpsLen returns the number of operations in the handler's dispatch table.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// AddAdapterInternal adds an adapter directly to the backend for test seeding.
func AddAdapterInternal(b *InMemoryBackend, a *Adapter) {
	b.mu.Lock("CreateAdapter")
	defer b.mu.Unlock()

	store := b.adaptersStore(b.region)
	store[a.AdapterID] = cloneAdapter(a)
}

// AddAdapterVersionInternal adds an adapter version directly to the backend for test seeding.
func AddAdapterVersionInternal(b *InMemoryBackend, av *AdapterVersion) {
	b.mu.Lock("CreateAdapterVersion")
	defer b.mu.Unlock()

	store := b.adapterVersionsStore(b.region)
	store[adapterVersionKey(av.AdapterID, av.AdapterVersion)] = cloneAdapterVersion(av)
}

// AddExpenseJobInternal adds an expense job directly to the backend for test seeding.
func AddExpenseJobInternal(b *InMemoryBackend, j *ExpenseJob) {
	b.mu.Lock("StartExpenseAnalysis")
	defer b.mu.Unlock()

	store := b.expenseJobsStore(b.region)
	store[j.JobID] = cloneExpenseJob(j)
}

// AddLendingJobInternal adds a lending job directly to the backend for test seeding.
func AddLendingJobInternal(b *InMemoryBackend, j *LendingJob) {
	b.mu.Lock("StartLendingAnalysis")
	defer b.mu.Unlock()

	store := b.lendingJobsStore(b.region)
	store[j.JobID] = cloneLendingJob(j)
}
